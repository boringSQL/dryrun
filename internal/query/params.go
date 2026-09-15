package query

import (
	"strconv"
	"strings"
)

// rewriteNamedParams rewrites :name to $n, padded to the original token length
// so parser offsets still address the caller's own SQL.
func rewriteNamedParams(sql string) string {
	if !strings.ContainsRune(sql, ':') {
		return sql
	}

	out := []byte(sql)
	ordinals := make(map[string]int)
	nextOrdinal := 0

	remap := func(name string) int {
		if o, ok := ordinals[name]; ok {
			return o
		}
		nextOrdinal++
		ordinals[name] = nextOrdinal
		return nextOrdinal
	}

	bracketDepth := 0
	var prevNonSpace byte

	for i := 0; i < len(sql); {
		c := sql[i]
		switch {
		case c == '-' && i+1 < len(sql) && sql[i+1] == '-':
			i += 2
			for i < len(sql) && sql[i] != '\n' {
				i++
			}
		case c == '/' && i+1 < len(sql) && sql[i+1] == '*':
			i = skipBlockComment(sql, i)
		case c == '\'':
			i = skipSingleQuoted(sql, i, escapeStringPrefix(sql, i))
			prevNonSpace = '\''
		case c == '"':
			i = skipDoubleQuoted(sql, i)
			prevNonSpace = '"'
		case c == '$':
			if end, ok := dollarQuoteEnd(sql, i); ok {
				i = end
				prevNonSpace = '$'
				continue
			}
			i++
			prevNonSpace = c
		case c == '[':
			bracketDepth++
			prevNonSpace = c
			i++
		case c == ']':
			if bracketDepth > 0 {
				bracketDepth--
			}
			prevNonSpace = c
			i++
		case c == ':':
			name, ok := paramName(sql, i, prevNonSpace, bracketDepth)
			if !ok {
				prevNonSpace = c
				i++
				continue
			}
			tokenLen := 1 + len(name)
			rep := "$" + strconv.Itoa(remap(name))
			if len(rep) > tokenLen {
				rep = "$1" // always fits a one-char name; a two-digit ordinal might not
			}
			copy(out[i:i+len(rep)], rep)
			for k := i + len(rep); k < i+tokenLen; k++ {
				out[k] = ' '
			}
			i += tokenLen
			prevNonSpace = rep[len(rep)-1]
		default:
			if c != ' ' && c != '\t' && c != '\n' && c != '\r' {
				prevNonSpace = c
			}
			i++
		}
	}
	return string(out)
}

// a slice separator always follows a complete expression; a parameter never does.
func isExprEnder(c byte) bool {
	return isIdentByte(c, false) || c == ')' || c == ']' || c == '\'' || c == '"'
}

// paramName reports the name of a :name parameter starting at the colon; the
// prev-byte guards tell it from a slice separator or a :: cast.
func paramName(sql string, i int, prevNonSpace byte, bracketDepth int) (string, bool) {
	if i+1 >= len(sql) || !isIdentByte(sql[i+1], true) {
		return "", false
	}
	if prevNonSpace == ':' {
		return "", false
	}
	if i > 0 && isIdentByte(sql[i-1], false) {
		return "", false
	}
	// a keyword before the colon (ARRAY[... THEN :x]) reads as an ender: accepted gap
	if bracketDepth > 0 && isExprEnder(prevNonSpace) {
		return "", false
	}
	j := i + 1
	for j < len(sql) && isIdentByte(sql[j], false) {
		j++
	}
	return sql[i+1 : j], true
}

func skipBlockComment(sql string, i int) int {
	i += 2
	depth := 1
	for i < len(sql) && depth > 0 {
		switch {
		case sql[i] == '/' && i+1 < len(sql) && sql[i+1] == '*':
			depth++
			i += 2
		case sql[i] == '*' && i+1 < len(sql) && sql[i+1] == '/':
			depth--
			i += 2
		default:
			i++
		}
	}
	return i
}

func skipSingleQuoted(sql string, i int, escapes bool) int {
	i++
	for i < len(sql) {
		switch {
		case escapes && sql[i] == '\\' && i+1 < len(sql):
			i += 2
		case sql[i] == '\'':
			if i+1 < len(sql) && sql[i+1] == '\'' {
				i += 2
				continue
			}
			return i + 1
		default:
			i++
		}
	}
	return i
}

func skipDoubleQuoted(sql string, i int) int {
	i++
	for i < len(sql) {
		if sql[i] == '"' {
			if i+1 < len(sql) && sql[i+1] == '"' {
				i += 2
				continue
			}
			return i + 1
		}
		i++
	}
	return i
}

// whether the quote at i opens an E or U& escape string, where a backslash
// escapes the next byte (B and X prefixes do not).
func escapeStringPrefix(sql string, i int) bool {
	if i == 0 {
		return false
	}
	switch sql[i-1] {
	case 'e', 'E':
		return i < 2 || !isIdentByte(sql[i-2], false)
	case '&':
		if i < 2 || (sql[i-2] != 'u' && sql[i-2] != 'U') {
			return false
		}
		return i < 3 || !isIdentByte(sql[i-3], false)
	}
	return false
}

// dollar-quote tags exclude '$' or the tag would swallow its own terminator.
func isTagByte(c byte) bool {
	return c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '_'
}

// a dollar-quote tag never starts with a digit, so $1 stays a positional parameter.
func dollarQuoteEnd(sql string, i int) (int, bool) {
	j := i + 1
	for j < len(sql) && isTagByte(sql[j]) {
		j++
	}
	if j >= len(sql) || sql[j] != '$' {
		return 0, false
	}
	if j > i+1 && sql[i+1] >= '0' && sql[i+1] <= '9' {
		return 0, false
	}
	tag := sql[i : j+1]
	k := j + 1
	idx := strings.Index(sql[k:], tag)
	if idx < 0 {
		return len(sql), true
	}
	return k + idx + len(tag), true
}
