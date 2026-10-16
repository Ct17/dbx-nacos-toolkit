package main

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"
)

// toolCallTimeout bounds a single tool execution (host limit is 120s).
const toolCallTimeout = 30 * time.Second

// mcpTools is the tool catalogue served to the DBX built-in AI agent and,
// because the manifest declares `external_tools: true`, to the external dbx
// MCP server as well. The host strips `connectionId` from the schema shown to
// the model and injects the bound connection when forwarding a call.
func mcpTools() map[string]any {
	connectionID := map[string]any{
		"type":        "string",
		"description": "Bound connection id (injected by the host).",
	}
	return map[string]any{
		"tools": []map[string]any{
			{
				"name": "get_config",
				"description": "Fetch the full content of one Nacos config by dataId. " +
					"Use this to read application/feature configs before answering questions about service behavior, " +
					"or to compare the same config across environments.",
				"inputSchema": map[string]any{
					"type": "object",
					"properties": map[string]any{
						"connectionId": connectionID,
						"dataId": map[string]any{
							"type":        "string",
							"description": "Config dataId, e.g. gateway.yaml",
						},
						"group": map[string]any{
							"type":        "string",
							"description": "Config group; defaults to DEFAULT_GROUP.",
						},
						"namespace": map[string]any{
							"type":        "string",
							"description": "Optional namespace id override; defaults to the connection namespace.",
						},
					},
					"required": []string{"connectionId", "dataId"},
				},
				"annotations": map[string]any{"readOnlyHint": true},
			},
			{
				"name": "search_configs",
				"description": "Search Nacos configs by dataId or group keyword and return paginated metadata " +
					"(dataId, group, type, md5; no content). Use this to discover which configs exist, " +
					"then fetch full content with get_config.",
				"inputSchema": map[string]any{
					"type": "object",
					"properties": map[string]any{
						"connectionId": connectionID,
						"dataId": map[string]any{
							"type":        "string",
							"description": "dataId keyword (substring match); omit to match all configs.",
						},
						"group": map[string]any{
							"type":        "string",
							"description": "Group keyword (substring match); omit to match all groups.",
						},
						"namespace": map[string]any{
							"type":        "string",
							"description": "Optional namespace id override; defaults to the connection namespace.",
						},
						"pageNo": map[string]any{
							"type":        "integer",
							"description": "Page number, starting at 1.",
							"minimum":     1,
						},
						"pageSize": map[string]any{
							"type":        "integer",
							"description": "Page size, 1-200.",
							"minimum":     1,
							"maximum":     200,
						},
					},
					"required": []string{"connectionId"},
				},
				"annotations": map[string]any{"readOnlyHint": true},
			},
			{
				"name": "list_namespaces",
				"description": "List all namespaces on the Nacos server with their config counts. " +
					"Use this to find a namespace id before querying configs or services in that environment.",
				"inputSchema": map[string]any{
					"type": "object",
					"properties": map[string]any{
						"connectionId": connectionID,
					},
					"required": []string{"connectionId"},
				},
				"annotations": map[string]any{"readOnlyHint": true},
			},
			{
				"name": "list_services",
				"description": "List registered service names in the connection namespace (service discovery view). " +
					"Use this to see which services exist in an environment.",
				"inputSchema": map[string]any{
					"type": "object",
					"properties": map[string]any{
						"connectionId": connectionID,
						"groupName": map[string]any{
							"type":        "string",
							"description": "Optional service group filter, e.g. DEFAULT_GROUP.",
						},
						"pageNo": map[string]any{
							"type":        "integer",
							"description": "Page number, starting at 1.",
							"minimum":     1,
						},
						"pageSize": map[string]any{
							"type":        "integer",
							"description": "Page size, 1-200.",
							"minimum":     1,
							"maximum":     200,
						},
					},
					"required": []string{"connectionId"},
				},
				"annotations": map[string]any{"readOnlyHint": true},
			},
		},
	}
}

// callTool executes one tool call. Failures become `isError` results so the
// model can read the problem and adjust, instead of protocol-level errors.
func (p *plugin) callTool(params map[string]any) map[string]any {
	tool, _ := params["tool"].(string)
	arguments := mapValue(params["arguments"])
	lifecycle := mapValue(params["lifecycle"])
	connection := mapValue(lifecycle["connection"])

	session, err := p.sessionFor(connection)
	if err != nil {
		return toolError(err.Error())
	}

	ctx, cancel := context.WithTimeout(context.Background(), toolCallTimeout)
	defer cancel()

	switch tool {
	case "get_config":
		dataID := stringArg(arguments, "dataId")
		if strings.TrimSpace(dataID) == "" {
			return toolError("dataId is required")
		}
		group := stringArg(arguments, "group")
		if group == "" {
			group = defaultGroup
		}
		namespace := stringArg(arguments, "namespace")
		if namespace == "" {
			namespace = session.namespace
		}
		content, err := session.client.getConfig(ctx, namespace, dataID, group)
		if err != nil {
			return toolError(err.Error())
		}
		return toolJSON(map[string]any{
			"connection": session.name,
			"dataId":     dataID,
			"group":      group,
			"namespace":  namespace,
			"content":    content,
		}, false)

	case "search_configs":
		namespace := stringArg(arguments, "namespace")
		if namespace == "" {
			namespace = session.namespace
		}
		page, err := session.client.searchConfigs(ctx, namespace,
			stringArg(arguments, "dataId"), stringArg(arguments, "group"),
			intArg(arguments, "pageNo"), intArg(arguments, "pageSize"))
		if err != nil {
			return toolError(err.Error())
		}
		return toolJSON(map[string]any{
			"connection":     session.name,
			"namespace":      namespace,
			"totalCount":     page.TotalCount,
			"pageNumber":     page.PageNumber,
			"pagesAvailable": page.PagesAvailable,
			"items":          page.PageItems,
		}, false)

	case "list_namespaces":
		namespaces, err := session.client.listNamespaces(ctx)
		if err != nil {
			return toolError(err.Error())
		}
		items := make([]map[string]any, 0, len(namespaces))
		for _, ns := range namespaces {
			items = append(items, map[string]any{
				"namespace":   ns.NamespaceID,
				"showName":    ns.ShowName,
				"configCount": ns.ConfigCount,
			})
		}
		return toolJSON(map[string]any{
			"connection": session.name,
			"items":      items,
		}, false)

	case "list_services":
		namespace := session.namespace
		page, err := session.client.listServices(ctx, namespace,
			stringArg(arguments, "groupName"), intArg(arguments, "pageNo"), intArg(arguments, "pageSize"))
		if err != nil {
			return toolError(err.Error())
		}
		services := page.services()
		if services == nil {
			services = []string{}
		}
		return toolJSON(map[string]any{
			"connection": session.name,
			"namespace":  namespace,
			"count":      page.Count,
			"services":   services,
		}, false)

	case "":
		return toolError("missing tool name")
	default:
		return toolError("unknown tool: " + tool)
	}
}

// connectionSession is a resolved view of one plugin connection.
type connectionSession struct {
	client    *nacosClient
	namespace string
	name      string
}

// sessionFor resolves a connection payload (from `lifecycle.connection`) into
// a usable client. Values missing from the payload fall back to the snapshot
// captured at connection/connect time.
func (p *plugin) sessionFor(connection map[string]any) (*connectionSession, error) {
	connectionID := stringArg(connection, "id")

	p.mu.Lock()
	cached, hasCached := p.connections[connectionID]
	p.mu.Unlock()

	host := connectionField(connection, "host")
	port := connectionPort(connection)
	contextPath := connectionField(connection, "context_path")
	namespace := connectionField(connection, "namespace")
	username := connectionField(connection, "username")
	password := connectionField(connection, "password")
	name := stringArg(connection, "name")

	if hasCached {
		if host == "" {
			host = cached.host
		}
		if port == 0 {
			port = cached.port
		}
		if contextPath == "" {
			contextPath = cached.contextPath
		}
		if namespace == "" {
			namespace = cached.namespace
		}
		if username == "" {
			username = cached.username
		}
		if password == "" {
			password = cached.password
		}
		if name == "" {
			name = cached.name
		}
	}
	if host == "" {
		return nil, fmt.Errorf("no usable Nacos connection: open (or re-open) a saved Nacos connection in DBX and pass its connectionId")
	}
	client, err := newNacosClient(host, port, contextPath, username, password)
	if err != nil {
		return nil, err
	}
	if name == "" {
		name = "Nacos"
	}
	return &connectionSession{client: client, namespace: namespace, name: name}, nil
}

// --- helpers ----------------------------------------------------------------

func toolJSON(payload map[string]any, isError bool) map[string]any {
	encoded, err := json.MarshalIndent(payload, "", "  ")
	if err != nil {
		return toolError("encoding result failed: " + err.Error())
	}
	return map[string]any{
		"content": []map[string]any{{"type": "text", "text": string(encoded)}},
		"isError": isError,
	}
}

func toolError(message string) map[string]any {
	return map[string]any{
		"content": []map[string]any{{"type": "text", "text": message}},
		"isError": true,
	}
}

func mapValue(raw any) map[string]any {
	if value, ok := raw.(map[string]any); ok {
		return value
	}
	return map[string]any{}
}

func stringArg(values map[string]any, key string) string {
	if value, ok := values[key].(string); ok {
		return strings.TrimSpace(value)
	}
	return ""
}

func intArg(values map[string]any, key string) int {
	switch value := values[key].(type) {
	case float64:
		return int(value)
	case int:
		return value
	case json.Number:
		if n, err := value.Int64(); err == nil {
			return int(n)
		}
	case string:
		if n, err := strconv.Atoi(strings.TrimSpace(value)); err == nil {
			return n
		}
	}
	return 0
}

// connectionField reads a value from the connection payload, falling back to
// the external_config map used by `binding: "config"` fields.
func connectionField(connection map[string]any, key string) string {
	if value, ok := connection[key].(string); ok && strings.TrimSpace(value) != "" {
		return strings.TrimSpace(value)
	}
	if ext := mapValue(connection["external_config"]); len(ext) > 0 {
		if value, ok := ext[key].(string); ok && strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}

func connectionPort(connection map[string]any) int {
	if port := intArg(connection, "port"); port > 0 {
		return port
	}
	if ext := mapValue(connection["external_config"]); len(ext) > 0 {
		return intArg(ext, "port")
	}
	return 0
}
