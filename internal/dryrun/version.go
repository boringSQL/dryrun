package dryrun

import (
	"fmt"
	"strconv"
	"strings"
	"unicode"
)

// The oldest major dryrun supports; tracks the oldest community-supported
// release. Older servers are warned about, not refused. Never use this to
// interpret a stored snapshot: it moves between binaries.
const MinSupportedMajor = 14

type PgVersion struct {
	Major int `json:"major"`
	Minor int `json:"minor"`
	Patch int `json:"patch"`
}

func (v PgVersion) String() string {
	return fmt.Sprintf("%d.%d.%d", v.Major, v.Minor, v.Patch)
}

// Parses output of SELECT version(), e.g. "PostgreSQL 17.2 on x86_64-..."
func ParsePgVersion(versionStr string) (PgVersion, error) {
	fields := strings.Fields(versionStr)

	var token string
	for _, f := range fields {
		t := strings.TrimRight(f, ",")
		if t == "" {
			continue
		}
		if unicode.IsDigit(rune(t[0])) && strings.Contains(t, ".") {
			token = t
			break
		}
	}
	if token == "" {
		return PgVersion{}, NewError(ErrVersionParse,
			fmt.Sprintf("no version token found in: %s", versionStr))
	}

	parts := strings.Split(token, ".")
	parsePart := func(s string) (int, error) {
		// leading digits only (e.g. "2beta1" -> 2)
		var digits []rune
		for _, r := range s {
			if unicode.IsDigit(r) {
				digits = append(digits, r)
			} else {
				break
			}
		}
		if len(digits) == 0 {
			return 0, NewError(ErrVersionParse,
				fmt.Sprintf("invalid version component: %s", s))
		}
		return strconv.Atoi(string(digits))
	}

	major, err := parsePart(parts[0])
	if err != nil {
		return PgVersion{}, err
	}

	var minor, patch int
	if len(parts) > 1 {
		minor, err = parsePart(parts[1])
		if err != nil {
			return PgVersion{}, err
		}
	}
	if len(parts) > 2 {
		patch, err = parsePart(parts[2])
		if err != nil {
			return PgVersion{}, err
		}
	}

	return PgVersion{Major: major, Minor: minor, Patch: patch}, nil
}
