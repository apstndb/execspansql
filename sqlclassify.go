package main

import (
	"strings"
)

// isDML reports whether sql is a DML statement (INSERT/UPDATE/DELETE) after
// stripping leading whitespace and SQL comments.
func isDML(sql string) bool {
	rest := stripLeadingSQLComments(sql)
	if rest == "" {
		return false
	}
	keyword, _, _ := strings.Cut(rest, " ")
	switch strings.ToUpper(keyword) {
	case "INSERT", "UPDATE", "DELETE":
		return true
	default:
		return false
	}
}

func stripLeadingSQLComments(s string) string {
	s = strings.TrimLeft(s, " \t\r\n")
	for s != "" {
		switch {
		case strings.HasPrefix(s, "--"):
			if i := strings.IndexByte(s, '\n'); i >= 0 {
				s = strings.TrimLeft(s[i+1:], " \t\r\n")
			} else {
				return ""
			}
		case strings.HasPrefix(s, "/*"):
			if i := strings.Index(s, "*/"); i >= 0 {
				s = strings.TrimLeft(s[i+2:], " \t\r\n")
			} else {
				return ""
			}
		default:
			return s
		}
	}
	return s
}
