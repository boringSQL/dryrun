package query

import (
	"fmt"
	"regexp"
	"strings"
)

type (
	MigrationFramework string

	MigrationSection struct {
		DDL           string
		NoTransaction bool
	}

	MigrationEnvelope struct {
		Framework MigrationFramework
		Up        MigrationSection
		Down      *MigrationSection
	}
)

const (
	FrameworkPlain  MigrationFramework = "plain"
	FrameworkGoose  MigrationFramework = "goose"
	FrameworkDbmate MigrationFramework = "dbmate"
	FrameworkTern   MigrationFramework = "tern"
)

var (
	gooseDirectiveRe  = regexp.MustCompile(`(?i)^\s*--\s*\+goose:?\s+(.*)$`)
	dbmateDirectiveRe = regexp.MustCompile(`(?i)^\s*--\s*migrate:\s*(up|down)\b(.*)$`)
	dbmateNoTxRe      = regexp.MustCompile(`(?i)\btransaction\s*:\s*false\b`)
	concurrentWordRe  = regexp.MustCompile(`(?i)\bCONCURRENTLY\b`)
)

const ternSeparator = "---- create above / drop below ----"

func ParseMigrationEnvelope(content string) MigrationEnvelope {
	lines := strings.Split(content, "\n")

	switch {
	case hasGooseDirective(lines):
		return parseGoose(lines)
	case hasDbmateDirective(lines):
		return parseDbmate(lines)
	case hasTernSeparator(lines):
		return parseTern(lines)
	default:
		return MigrationEnvelope{
			Framework: FrameworkPlain,
			Up:        MigrationSection{DDL: content},
		}
	}
}

func (e MigrationEnvelope) Section(direction string) (MigrationSection, error) {
	switch strings.ToLower(strings.TrimSpace(direction)) {
	case "", "up":
		return e.Up, nil
	case "down":
		if e.Down == nil {
			if e.Framework == FrameworkPlain {
				return MigrationSection{}, fmt.Errorf("cannot check a 'down' migration: plain SQL has no down section (goose, dbmate and tern files carry one)")
			}
			return MigrationSection{}, fmt.Errorf("cannot check a 'down' migration: this %s file has no down section", e.Framework)
		}
		return *e.Down, nil
	default:
		return MigrationSection{}, fmt.Errorf("invalid direction %q: must be 'up' or 'down'", direction)
	}
}

func hasGooseDirective(lines []string) bool {
	for _, line := range lines {
		if m := gooseDirectiveRe.FindStringSubmatch(line); m != nil {
			switch normalizeGooseDirective(m[1]) {
			case "UP", "DOWN", "NO TRANSACTION":
				return true
			}
		}
	}
	return false
}

func hasDbmateDirective(lines []string) bool {
	for _, line := range lines {
		if dbmateDirectiveRe.MatchString(line) {
			return true
		}
	}
	return false
}

func hasTernSeparator(lines []string) bool {
	for _, line := range lines {
		if strings.EqualFold(strings.TrimSpace(line), ternSeparator) {
			return true
		}
	}
	return false
}

// leading NO TRANSACTION is file-level; content before first marker is up
func parseGoose(lines []string) MigrationEnvelope {
	env := MigrationEnvelope{Framework: FrameworkGoose}

	section := ""
	globalNoTx := false
	var upNoTx, downNoTx bool
	sawDown := false
	var up, down []string

	for _, raw := range lines {
		line := strings.TrimSuffix(raw, "\r")
		if m := gooseDirectiveRe.FindStringSubmatch(line); m != nil {
			switch normalizeGooseDirective(m[1]) {
			case "UP":
				section = "up"
			case "DOWN":
				section = "down"
				sawDown = true
			case "NO TRANSACTION":
				switch section {
				case "up":
					upNoTx = true
				case "down":
					downNoTx = true
				default:
					globalNoTx = true
				}
			}
			continue
		}
		if section == "down" {
			down = append(down, line)
		} else {
			up = append(up, line)
		}
	}

	env.Up = MigrationSection{DDL: strings.Join(up, "\n"), NoTransaction: upNoTx || globalNoTx}
	if sawDown {
		env.Down = &MigrationSection{DDL: strings.Join(down, "\n"), NoTransaction: downNoTx || globalNoTx}
	}
	return env
}

func normalizeGooseDirective(rest string) string {
	fields := strings.Fields(rest)
	if len(fields) == 0 {
		return ""
	}
	if strings.EqualFold(fields[0], "NO") && len(fields) > 1 && strings.EqualFold(fields[1], "TRANSACTION") {
		return "NO TRANSACTION"
	}
	return strings.ToUpper(fields[0])
}

func parseDbmate(lines []string) MigrationEnvelope {
	env := MigrationEnvelope{Framework: FrameworkDbmate}

	section := ""
	var upNoTx, downNoTx bool
	sawDown := false
	var up, down []string

	for _, raw := range lines {
		line := strings.TrimSuffix(raw, "\r")
		if m := dbmateDirectiveRe.FindStringSubmatch(line); m != nil {
			noTx := dbmateNoTransaction(m[2])
			if strings.EqualFold(m[1], "up") {
				section = "up"
				upNoTx = noTx
			} else {
				section = "down"
				sawDown = true
				downNoTx = noTx
			}
			continue
		}
		if section == "down" {
			down = append(down, line)
		} else {
			up = append(up, line)
		}
	}

	env.Up = MigrationSection{DDL: strings.Join(up, "\n"), NoTransaction: upNoTx}
	if sawDown {
		env.Down = &MigrationSection{DDL: strings.Join(down, "\n"), NoTransaction: downNoTx}
	}
	return env
}

func dbmateNoTransaction(rest string) bool {
	return dbmateNoTxRe.MatchString(rest)
}

func parseTern(lines []string) MigrationEnvelope {
	env := MigrationEnvelope{Framework: FrameworkTern}

	inDown := false
	var up, down []string
	for _, raw := range lines {
		line := strings.TrimSuffix(raw, "\r")
		if strings.EqualFold(strings.TrimSpace(line), ternSeparator) {
			inDown = true
			continue
		}
		if inDown {
			down = append(down, line)
		} else {
			up = append(up, line)
		}
	}

	env.Up = MigrationSection{DDL: strings.Join(up, "\n")}
	env.Down = &MigrationSection{DDL: strings.Join(down, "\n")}
	return env
}

// Returns "" when no runnable file exists (tern cannot run CONCURRENTLY).
func FormatMigrationFile(env MigrationEnvelope, direction, rewritten string) string {
	if rewritten == "" {
		return ""
	}
	switch env.Framework {
	case FrameworkGoose:
		return formatGoose(env, direction, rewritten)
	case FrameworkDbmate:
		return formatDbmate(env, direction, rewritten)
	case FrameworkTern:
		return formatTern(env, direction, rewritten)
	default:
		return rewritten
	}
}

func formatGoose(env MigrationEnvelope, direction, rewritten string) string {
	sec, err := env.Section(direction)
	if err != nil {
		return rewritten
	}
	noTx := sec.NoTransaction || containsConcurrently(rewritten)

	var b strings.Builder
	if noTx {
		b.WriteString("-- +goose NO TRANSACTION\n")
	}
	if downward(direction) {
		b.WriteString("-- +goose Up\n")
		b.WriteString(withTrailingNewline(env.Up.DDL))
		b.WriteString("-- +goose Down\n")
		b.WriteString(withTrailingNewline(rewritten))
		return b.String()
	}
	b.WriteString("-- +goose Up\n")
	b.WriteString(withTrailingNewline(rewritten))
	if env.Down != nil {
		b.WriteString("-- +goose Down\n")
		b.WriteString(withTrailingNewline(env.Down.DDL))
	}
	return b.String()
}

func formatDbmate(env MigrationEnvelope, direction, rewritten string) string {
	sec, err := env.Section(direction)
	if err != nil {
		return rewritten
	}
	noTx := sec.NoTransaction || containsConcurrently(rewritten)

	upMarker := "-- migrate:up"
	if !downward(direction) && noTx {
		upMarker += " transaction:false"
	}
	downMarker := "-- migrate:down"
	if downward(direction) && noTx {
		downMarker += " transaction:false"
	}

	var b strings.Builder
	b.WriteString(upMarker + "\n")
	if downward(direction) {
		b.WriteString(withTrailingNewline(env.Up.DDL))
		b.WriteString(downMarker + "\n")
		b.WriteString(withTrailingNewline(rewritten))
		return b.String()
	}
	b.WriteString(withTrailingNewline(rewritten))
	if env.Down != nil {
		b.WriteString(downMarker + "\n")
		b.WriteString(withTrailingNewline(env.Down.DDL))
	}
	return b.String()
}

func formatTern(env MigrationEnvelope, direction, rewritten string) string {
	if containsConcurrently(rewritten) {
		return ""
	}
	var b strings.Builder
	if downward(direction) {
		b.WriteString(withTrailingNewline(env.Up.DDL))
	} else {
		b.WriteString(withTrailingNewline(rewritten))
	}
	if env.Down == nil && !downward(direction) {
		return b.String()
	}
	b.WriteString(ternSeparator + "\n")
	if downward(direction) {
		b.WriteString(withTrailingNewline(rewritten))
	} else {
		b.WriteString(withTrailingNewline(env.Down.DDL))
	}
	return b.String()
}

func containsConcurrently(s string) bool {
	return concurrentWordRe.MatchString(s)
}

// MarkConcurrentInTransaction flips safe CONCURRENTLY checks to dangerous when
// the runner wraps this section in a transaction: the statement fails at apply.
// SaferSQL is the statement itself -- the fix is the no-transaction marker
// FormatMigrationFile injects; tern has no opt-out, so it gets no rewrite.
// Mutates checks in place.
func MarkConcurrentInTransaction(env MigrationEnvelope, section MigrationSection, checks []MigrationCheck) {
	if section.NoTransaction || env.Framework == FrameworkPlain {
		return
	}
	for i := range checks {
		c := &checks[i]
		if c.Safety != SafetySafe || !concurrentWordRe.MatchString(c.Operation) {
			continue
		}
		c.Safety = SafetyDangerous
		c.Rationale = &Rationale{Reason: fmt.Sprintf("%s cannot run inside the transaction %s wraps this file in.", c.Operation, env.Framework)}
		c.Recommendation = c.Rationale.Reason + " " + concurrentTransactionFix(env.Framework)
		// tern has no opt-out, so no rewrite to offer; the marker-adding
		// frameworks get the statement passed through as SaferSQL.
		if c.Statement != "" && env.Framework != FrameworkTern {
			c.SaferSQL = []string{c.Statement}
		}
	}
}

func concurrentTransactionFix(framework MigrationFramework) string {
	switch framework {
	case FrameworkGoose:
		return "Add `-- +goose NO TRANSACTION` at the top of the file so the statement runs outside it."
	case FrameworkDbmate:
		return "Add `transaction:false` to the section's migrate marker so the statement runs outside it."
	default:
		return "There is no opt-out, so run this statement out-of-band."
	}
}

func downward(direction string) bool {
	return strings.EqualFold(strings.TrimSpace(direction), "down")
}

func withTrailingNewline(s string) string {
	if s == "" || strings.HasSuffix(s, "\n") {
		return s
	}
	return s + "\n"
}
