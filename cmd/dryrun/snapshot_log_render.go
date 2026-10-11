package main

import (
	"cmp"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/boringsql/dryrun/pkg/diff"
)

const (
	maxChangeLines  = 6
	maxCaptureLines = 3
	foldTableAt     = 5 // child changes before a table collapses to a count
	headerWidth     = 80
	onelineWidth    = 100
)

type (
	changeLine struct {
		text    string
		changes int // how many diff changes the line stands for
		repeat  int // identical consecutive lines (function overloads)
	}

	logView struct {
		Captures bool // off for --kind schema
		Quiet    bool // hide "no captures" under a --node/--kind filter
		Patch    bool // every change line in full: no budget, fold, hint or clip
	}
)

func renderLog(w io.Writer, lg schemaLog, view logView) {
	for i, v := range lg.Versions {
		if i > 0 {
			fmt.Fprintln(w)
		}
		renderVersion(w, v, view)
	}
	if view.Captures && len(lg.Orphans) > 0 {
		if len(lg.Versions) > 0 {
			fmt.Fprintln(w)
		}
		fmt.Fprintf(w, "captures with an unknown schema (%d)\n", len(lg.Orphans))
		renderCaptures(w, lg.Orphans)
	}
	renderOlder(w, lg.Older)
}

func renderVersion(w io.Writer, v schemaVersion, view logView) {
	left := fmt.Sprintf("schema %s  (%s)", hash7(v.Row.ContentHash), refToken(v.Ref))
	n, right := versionSummary(v)
	if n >= 0 {
		right = fmt.Sprintf("%d %s", n, right)
	}
	fmt.Fprintf(w, "%s%s%s\n", left, strings.Repeat(" ", max(2, headerWidth-len(left)-len(right))), right)

	date := "Date:   " + v.Row.Timestamp.UTC().Format("Mon 2006-01-02 15:04:05 UTC")
	if v.Ref > 0 {
		date += " · lived " + fmtLived(v.Lived)
	}
	fmt.Fprintln(w, date)

	switch {
	case v.DiffErr != nil:
		fmt.Fprintf(w, "    diff unavailable: %v\n", v.DiffErr)
	case v.Delta.IsEmpty() && !v.Oldest:
		fmt.Fprintln(w, "    no schema change")
	case !v.Oldest:
		renderChanges(w, v, view.Patch)
	}

	if !view.Captures || (len(v.Captures) == 0 && view.Quiet) {
		return
	}
	fmt.Fprintln(w)
	if len(v.Captures) == 0 {
		fmt.Fprintln(w, "    no captures")
		return
	}
	renderCaptures(w, v.Captures)
}

func renderChanges(w io.Writer, v schemaVersion, patch bool) {
	if patch {
		lines, _ := changeLines(v.Delta.Changes, false, true)
		for _, l := range lines {
			fmt.Fprintln(w, "    "+l.text)
		}
		return
	}
	lines, folded := changeLines(v.Delta.Changes, false, false)
	if len(lines) > maxChangeLines {
		lines, folded = changeLines(v.Delta.Changes, true, false)
	}
	if len(lines) > maxChangeLines {
		lines, folded = lines[:maxChangeLines], true
		// a table header with none of its changes under it says nothing
		if lines[len(lines)-1].changes == 0 {
			lines = lines[:len(lines)-1]
		}
	}
	shown := 0
	for _, l := range lines {
		fmt.Fprintln(w, clip("    "+l.text, onelineWidth))
		shown += l.changes
	}
	if folded {
		more := ""
		if rest := len(v.Delta.Changes) - shown; rest > 0 {
			more = fmt.Sprintf("%d more · ", rest)
		}
		fmt.Fprintf(w, "    … %sdryrun snapshot diff %s %s\n", more, refToken(v.Ref+1), refToken(v.Ref))
	}
}

// Same grouping and wording as `snapshot diff`; fold collapses busy tables to a
// count, full keeps function argument lists.
func changeLines(changes []diff.Change, fold, full bool) (lines []changeLine, folded bool) {
	add := func(text string, n int) {
		if k := len(lines) - 1; k >= 0 && lines[k].text == text {
			lines[k].changes += n
			lines[k].repeat++
			return
		}
		lines = append(lines, changeLine{text: text, changes: n, repeat: 1})
	}
	for _, g := range diff.GroupByObject(changes) {
		if len(g) == 1 && diff.IsObjectLevel(g[0]) {
			c := g[0]
			text := fmt.Sprintf("%s %s %s", diff.Marker(c), c.Object.Kind, objectName(c.Object, full))
			if c.Note != "" {
				text += " (" + c.Note + ")"
			}
			add(text, 1)
			continue
		}
		head := fmt.Sprintf("~ %s %s", g[0].Object.Kind, objectName(g[0].Object, full))
		if fold && len(g) >= foldTableAt {
			add(fmt.Sprintf("%-40s %d changes", head, len(g)), len(g))
			folded = true
			continue
		}
		add(head, 0)
		for _, c := range g {
			add("    "+diff.Marker(c)+" "+diff.Describe(c), 1)
		}
	}
	for i := range lines {
		if lines[i].repeat > 1 {
			lines[i].text += fmt.Sprintf(" ×%d", lines[i].repeat)
		}
	}
	return lines, folded
}

func renderCaptures(w io.Writer, caps []capture) {
	for i, c := range caps {
		lead := "              "
		if i == 0 {
			lead = "    captured  "
		}
		if i == maxCaptureLines {
			fmt.Fprintf(w, "%s… %d more since %s\n", lead, len(caps)-i, caps[len(caps)-1].At.UTC().Format("2006-01-02 15:04"))
			return
		}
		fmt.Fprintf(w, "%s%s  %s\n", lead, c.At.UTC().Format("2006-01-02 15:04"), cmp.Or(strings.Join(c.Nodes, ", "), "planner"))
	}
}

func renderOneline(w io.Writer, lg schemaLog) {
	for _, v := range lg.Versions {
		n, summary := versionSummary(v)
		if n > 0 {
			summary = onelineSummary(v.Delta.Changes)
		}
		count := "-"
		if n >= 0 {
			count = fmt.Sprint(n)
		}
		fmt.Fprintf(w, "%s  %s  %-9s %3s  %s\n", hash7(v.Row.ContentHash),
			v.Row.Timestamp.UTC().Format("2006-01-02 15:04"), refToken(v.Ref), count, summary)
	}
	if len(lg.Orphans) > 0 {
		fmt.Fprintf(w, "%d %s with an unknown schema\n", len(lg.Orphans), pluralWord(len(lg.Orphans), "capture", "captures"))
	}
	renderOlder(w, lg.Older)
}

func onelineSummary(changes []diff.Change) string {
	var parts []string
	for _, g := range diff.GroupByObject(changes) {
		marker := "~"
		if len(g) == 1 && diff.IsObjectLevel(g[0]) {
			marker = diff.Marker(g[0])
		}
		p := marker + " " + objectName(g[0].Object, false)
		if len(parts) == 0 || parts[len(parts)-1] != p {
			parts = append(parts, p)
		}
	}
	for k := len(parts); k > 1; k-- {
		if out := strings.Join(parts[:k], ", "); len(out) <= onelineWidth-40 {
			if k < len(parts) {
				out += fmt.Sprintf(", +%d more", len(parts)-k)
			}
			return out
		}
	}
	return clip(parts[0], onelineWidth-40)
}

func renderOlder(w io.Writer, older int) {
	if older > 0 {
		fmt.Fprintf(w, "\n… %d older %s (--limit 0 for all)\n", older, pluralWord(older, "version", "versions"))
	}
}

// n is -1 when there is no diff to count
func versionSummary(v schemaVersion) (n int, label string) {
	switch {
	case v.DiffErr != nil:
		return -1, "diff unavailable"
	case v.Oldest:
		return -1, "oldest kept"
	case v.Delta.IsEmpty():
		return 0, "no schema change"
	}
	n = len(v.Delta.Changes)
	return n, pluralWord(n, "change", "changes")
}

// public is the common case; functions collapse their argument list unless full.
func objectName(r diff.ObjectRef, full bool) string {
	name := strings.TrimPrefix(r.Qualified(), "public.")
	if r.Kind == "function" && !full {
		if i := strings.IndexByte(name, '('); i >= 0 {
			name = name[:i] + "()"
		}
	}
	return name
}

// RETURNS TABLE(...) and long CHECK bodies would otherwise run for hundreds of columns
func clip(s string, width int) string {
	if r := []rune(s); len(r) > width {
		return string(r[:width-1]) + "…"
	}
	return s
}

func refToken(ref int) string {
	if ref == 0 {
		return "latest"
	}
	return fmt.Sprintf("latest~%d", ref)
}

func hash7(h string) string {
	if len(h) > 7 {
		return h[:7]
	}
	return h
}

func pluralWord(n int, one, many string) string {
	if n == 1 {
		return one
	}
	return many
}

func fmtLived(d time.Duration) string {
	const day = 24 * time.Hour
	h, m := int(d.Hours()), int(d.Minutes())%60
	switch {
	case d < time.Minute:
		return "<1m"
	case d < time.Hour:
		return fmt.Sprintf("%dm", m)
	case d < 6*time.Hour && m > 0:
		return fmt.Sprintf("%dh %dm", h, m)
	case d < day:
		return fmt.Sprintf("%dh", h)
	case d < 10*day && h%24 > 0:
		return fmt.Sprintf("%dd %dh", h/24, h%24)
	}
	return fmt.Sprintf("%dd", h/24)
}
