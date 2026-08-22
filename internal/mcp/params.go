package mcp

import "github.com/mark3labs/mcp-go/mcp"

func getArg(req mcp.CallToolRequest, key string) string {
	args := req.GetArguments()
	if args == nil {
		return ""
	}
	v, ok := args[key]
	if !ok {
		return ""
	}
	s, _ := v.(string)
	return s
}

func getFloatArg(req mcp.CallToolRequest, key string, fallback float64) float64 {
	args := req.GetArguments()
	if args == nil {
		return fallback
	}
	v, ok := args[key]
	if !ok {
		return fallback
	}
	f, _ := v.(float64)
	if f <= 0 {
		return fallback
	}
	return f
}

func getBoolArgDefault(req mcp.CallToolRequest, key string, fallback bool) bool {
	args := req.GetArguments()
	if args == nil {
		return fallback
	}
	v, ok := args[key]
	if !ok || v == nil {
		return fallback
	}
	b, ok := v.(bool)
	if !ok {
		return fallback
	}
	return b
}

func getBoolArg(req mcp.CallToolRequest, key string) bool {
	args := req.GetArguments()
	if args == nil {
		return false
	}
	v, ok := args[key]
	if !ok {
		return false
	}
	b, _ := v.(bool)
	return b
}

// Lookup tools answer about one object and need a schema to resolve against.
func schemaArg(req mcp.CallToolRequest) string {
	return argOr(req, "schema", "public")
}

// Filter tools must not default: absent means every schema, not "public".
func schemaFilterArg(req mcp.CallToolRequest) string {
	return getArg(req, "schema")
}

func argOr(req mcp.CallToolRequest, key, fallback string) string {
	if v := getArg(req, key); v != "" {
		return v
	}
	return fallback
}

// nil when absent; empty slice when present but empty.
func getStringSliceArg(req mcp.CallToolRequest, key string) []string {
	args := req.GetArguments()
	if args == nil {
		return nil
	}
	v, ok := args[key]
	if !ok || v == nil {
		return nil
	}
	switch raw := v.(type) {
	case []string:
		return raw
	case []any:
		out := make([]string, 0, len(raw))
		for _, e := range raw {
			if s, ok := e.(string); ok {
				out = append(out, s)
			}
		}
		return out
	}
	return nil
}

func pageEnd(offset, limit, total int) int {
	if limit > 0 && offset+limit < total {
		return offset + limit
	}
	return total
}
