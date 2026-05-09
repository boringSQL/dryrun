package mcp

import "github.com/mark3labs/mcp-go/mcp"

func tool(name, description string) mcp.Tool {
	return mcp.Tool{Name: name, Description: description}
}

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

func schemaArg(req mcp.CallToolRequest) string {
	return argOr(req, "schema", "public")
}

func argOr(req mcp.CallToolRequest, key, fallback string) string {
	if v := getArg(req, key); v != "" {
		return v
	}
	return fallback
}

func pageEnd(offset, limit, total int) int {
	if limit > 0 && offset+limit < total {
		return offset + limit
	}
	return total
}
