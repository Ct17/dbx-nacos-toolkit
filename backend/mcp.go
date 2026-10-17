package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"
)

// toolCallTimeout bounds a single tool execution (host limit is 120s).
const toolCallTimeout = 30 * time.Second

// toolResultMaxBytes keeps a tool result well below the sidecar SDK's 8 MiB
// protocol frame limit. If a result exceeded that limit the SDK could not write
// the response at all and the host would only see a timeout instead of an
// error, so oversized results are reported as a readable tool error here.
const toolResultMaxBytes = 6 << 20

// sessionCacheLimit bounds the per-connection client cache. Sessions are cheap
// to rebuild, so overflowing simply drops the cache instead of growing without
// bound.
const sessionCacheLimit = 64

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
							"description": "Page size, 1-500.",
							"minimum":     1,
							"maximum":     500,
						},
					},
					"required": []string{"connectionId"},
				},
				"annotations": map[string]any{"readOnlyHint": true},
			},
			{
				"name": "diff_config",
				"description": "Compare the same config across two to eight Nacos namespaces and return a line-level diff " +
					"against the first namespace (the baseline). Use this to answer questions like whether dev and test " +
					"share the same settings, or what changed between environments.",
				"inputSchema": map[string]any{
					"type": "object",
					"properties": map[string]any{
						"connectionId": connectionID,
						"dataId": map[string]any{
							"type":        "string",
							"description": "Config dataId to compare, e.g. gateway.yaml",
						},
						"group": map[string]any{
							"type":        "string",
							"description": "Config group; defaults to DEFAULT_GROUP.",
						},
						"namespaces": map[string]any{
							"type":        "array",
							"description": "Two to eight namespace ids; the first one is the baseline the others are compared against.",
							"items":       map[string]any{"type": "string"},
							"minItems":    2,
							"maxItems":    8,
						},
					},
					"required": []string{"connectionId", "dataId", "namespaces"},
				},
				"annotations": map[string]any{"readOnlyHint": true},
			},
			{
				"name": "get_config_history",
				"description": "List the change history of one Nacos config (who changed it and when), newest first. " +
					"Pass a historyId from the list to fetch that revision's full content instead. " +
					"opType values: I=insert, U=update, D=delete.",
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
						"historyId": map[string]any{
							"type":        "string",
							"description": "Fetch the full content of this revision instead of listing the history.",
						},
						"pageNo": map[string]any{
							"type":        "integer",
							"description": "Page number, starting at 1 (list mode only).",
							"minimum":     1,
						},
						"pageSize": map[string]any{
							"type":        "integer",
							"description": "Page size, 1-500 (list mode only).",
							"minimum":     1,
							"maximum":     500,
						},
					},
					"required": []string{"connectionId", "dataId"},
				},
				"annotations": map[string]any{"readOnlyHint": true},
			},
			{
				"name": "find_in_configs",
				"description": "Search Nacos config *content* (not just dataId/group) and return matching line snippets. " +
					"Use this when you know a property or value but not which config contains it. Nacos 3.x uses native " +
					"server-side content search; Nacos 1.x/2.x scans config metadata and content up to scanLimit, so check " +
					"truncated/truncatedReason before concluding a config does not exist.",
				"inputSchema": map[string]any{
					"type": "object",
					"properties": map[string]any{
						"connectionId": connectionID,
						"keyword": map[string]any{
							"type":        "string",
							"description": "Literal text to find in config content, e.g. spring.datasource.url.",
						},
						"namespace": map[string]any{
							"type":        "string",
							"description": "Optional namespace id override; defaults to the connection namespace.",
						},
						"dataId": map[string]any{
							"type":        "string",
							"description": "Optional dataId keyword to narrow the scan, e.g. gateway.",
						},
						"group": map[string]any{
							"type":        "string",
							"description": "Optional group keyword to narrow the scan.",
						},
						"maxResults": map[string]any{
							"type":        "integer",
							"description": "Maximum matching configs to return, 1-50 (default 20).",
							"minimum":     1,
							"maximum":     50,
						},
						"scanLimit": map[string]any{
							"type":        "integer",
							"description": "Maximum candidate configs to inspect on v1 (1-1000, default 200); ignored on v3.",
							"minimum":     1,
							"maximum":     1000,
						},
						"caseSensitive": map[string]any{
							"type":        "boolean",
							"description": "Match case-sensitively; defaults to false.",
						},
					},
					"required": []string{"connectionId", "keyword"},
				},
				"annotations": map[string]any{"readOnlyHint": true},
			},
			{
				"name": "diff_envs",
				"description": "Compare all configs across two to eight namespaces by md5 and report missing, extra and " +
					"changed configs. Use this for environment drift questions (dev vs test vs prod); follow up with " +
					"diff_config or diff_history to inspect one config.",
				"inputSchema": map[string]any{
					"type": "object",
					"properties": map[string]any{
						"connectionId": connectionID,
						"namespaces": map[string]any{
							"type":        "array",
							"description": "Two to eight namespace ids; the first one is the baseline.",
							"items":       map[string]any{"type": "string"},
							"minItems":    2,
							"maxItems":    8,
						},
						"dataId": map[string]any{
							"type":        "string",
							"description": "Optional dataId keyword to limit the comparison.",
						},
						"group": map[string]any{
							"type":        "string",
							"description": "Optional group keyword to limit the comparison.",
						},
						"includeContentDiff": map[string]any{
							"type":        "boolean",
							"description": "Generate line diffs for a bounded number of changed configs; defaults to false.",
						},
						"maxConfigsPerNamespace": map[string]any{
							"type":        "integer",
							"description": "Maximum configs to read per namespace, 1-2000 (default 1000).",
							"minimum":     1,
							"maximum":     2000,
						},
						"maxDifferences": map[string]any{
							"type":        "integer",
							"description": "Maximum difference entries to return, 1-500 (default 100).",
							"minimum":     1,
							"maximum":     500,
						},
					},
					"required": []string{"connectionId", "namespaces"},
				},
				"annotations": map[string]any{"readOnlyHint": true},
			},
			{
				"name": "diff_history",
				"description": "Compare one historical revision of a config against its current content (or against another " +
					"revision) and return a line-level diff plus operator/time metadata. Use get_config_history first to " +
					"obtain historyId values.",
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
						"fromHistoryId": map[string]any{
							"type":        "string",
							"description": "The historical revision to use as the baseline (from get_config_history).",
						},
						"toHistoryId": map[string]any{
							"type":        "string",
							"description": "Optional target revision; omit to compare against the current config.",
						},
						"contextLines": map[string]any{
							"type":        "integer",
							"description": "Context lines around each diff block, 1-10 (default 3).",
							"minimum":     1,
							"maximum":     10,
						},
					},
					"required": []string{"connectionId", "dataId", "fromHistoryId"},
				},
				"annotations": map[string]any{"readOnlyHint": true},
			},
			{
				"name": "diagnose",
				"description": "Run a read-only health check of the Nacos connection: protocol stack, server version/mode, " +
					"authentication, console port, admin initialization, namespace access and the config data plane. " +
					"Returns per-check statuses and remediation suggestions. Use this when a Nacos tool fails or during onboarding.",
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
							"description": "Page size, 1-500.",
							"minimum":     1,
							"maximum":     500,
						},
					},
					"required": []string{"connectionId"},
				},
				"annotations": map[string]any{"readOnlyHint": true},
			},
			{
				"name": "list_instances",
				"description": "List the instances of one registered service in the connection namespace, including " +
					"health state, enabled flag, weight and cluster. Use this after list_services to inspect which " +
					"nodes currently serve a service.",
				"inputSchema": map[string]any{
					"type": "object",
					"properties": map[string]any{
						"connectionId": connectionID,
						"serviceName": map[string]any{
							"type":        "string",
							"description": "Registered service name, e.g. order-service.",
						},
						"groupName": map[string]any{
							"type":        "string",
							"description": "Optional service group; defaults to DEFAULT_GROUP.",
						},
						"cluster": map[string]any{
							"type":        "string",
							"description": "Optional cluster name filter, e.g. DEFAULT.",
						},
						"namespace": map[string]any{
							"type":        "string",
							"description": "Optional namespace id override; defaults to the connection namespace.",
						},
					},
					"required": []string{"connectionId", "serviceName"},
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

	case "diff_config":
		dataID := stringArg(arguments, "dataId")
		if dataID == "" {
			return toolError("dataId is required")
		}
		group := stringArg(arguments, "group")
		if group == "" {
			group = defaultGroup
		}
		namespaces := stringArray(arguments, "namespaces")
		if len(namespaces) < 2 {
			return toolError("namespaces requires at least two entries; the first one is the baseline")
		}
		if len(namespaces) > 8 {
			return toolError("at most 8 namespaces per diff call")
		}

		// 并行读取所有命名空间的配置内容（2-8 个），再统一生成对比结果。
		// 远端服务器下可把 n 次往返压缩为 1 次往返耗时；任一命名空间读取
		// 失败（网络/权限）直接作为工具错误返回（按命名空间顺序取首个错误，
		// 保证信息稳定可复现）。
		type reading struct {
			namespace string
			content   string
			exists    bool
			err       error
		}
		readings := make([]reading, len(namespaces))
		var waitGroup sync.WaitGroup
		for index, namespace := range namespaces {
			waitGroup.Add(1)
			go func(index int, namespace string) {
				defer waitGroup.Done()
				content, exists, err := session.client.readConfig(ctx, namespace, dataID, group)
				readings[index] = reading{namespace: namespace, content: content, exists: exists, err: err}
			}(index, namespace)
		}
		waitGroup.Wait()
		for _, item := range readings {
			if item.err != nil {
				return toolError(fmt.Sprintf("reading namespace %q failed: %v", item.namespace, item.err))
			}
		}

		baseline := readings[0]
		comparisons := make([]map[string]any, 0, len(readings)-1)
		for _, target := range readings[1:] {
			entry := map[string]any{"namespace": target.namespace}
			switch {
			case !baseline.exists && !target.exists:
				entry["status"] = "missing-in-both"
			case !baseline.exists:
				entry["status"] = "missing-in-baseline"
			case !target.exists:
				entry["status"] = "missing-in-target"
			default:
				diffs, err := lineDiffs(baseline.content, target.content)
				if err != nil {
					return toolError(err.Error())
				}
				added, removed := countChanges(diffs)
				if added == 0 && removed == 0 {
					entry["status"] = "identical"
				} else {
					entry["status"] = "different"
					entry["added"] = added
					entry["removed"] = removed
					entry["diff"] = renderDiff(diffs, diffContext)
				}
			}
			comparisons = append(comparisons, entry)
		}
		return toolJSON(map[string]any{
			"connection": session.name,
			"dataId":     dataID,
			"group":      group,
			"baseline": map[string]any{
				"namespace": baseline.namespace,
				"exists":    baseline.exists,
			},
			"comparisons": comparisons,
		}, false)

	case "get_config_history":
		dataID := stringArg(arguments, "dataId")
		if dataID == "" {
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
		// 指定 historyId 时进入详情模式，否则返回历史列表。
		if historyID := stringArg(arguments, "historyId"); historyID != "" {
			detail, err := session.client.getConfigRevision(ctx, namespace, dataID, group, historyID)
			if err != nil {
				return toolError(err.Error())
			}
			return toolJSON(map[string]any{
				"connection":  session.name,
				"dataId":      detail.DataID,
				"group":       detail.Group,
				"namespace":   namespace,
				"historyId":   string(detail.ID),
				"opType":      strings.TrimSpace(detail.OpType),
				"operator":    detail.SrcUser,
				"srcIp":       detail.SrcIP,
				"md5":         detail.Md5,
				"createdTime": formatMillis(detail.revisionTime()),
				"content":     detail.Content,
			}, false)
		}
		page, err := session.client.listConfigHistory(ctx, namespace, dataID, group,
			intArg(arguments, "pageNo"), intArg(arguments, "pageSize"))
		if err != nil {
			return toolError(err.Error())
		}
		revisions := make([]map[string]any, 0, len(page.PageItems))
		for _, revision := range page.PageItems {
			at := revision.revisionTime()
			entry := map[string]any{
				"historyId":   string(revision.ID),
				"opType":      strings.TrimSpace(revision.OpType),
				"operator":    revision.SrcUser,
				"srcIp":       revision.SrcIP,
				"md5":         revision.Md5,
				"createdTime": formatMillis(at),
			}
			if at > 0 {
				entry["createdTimeMillis"] = at
			}
			revisions = append(revisions, entry)
		}
		return toolJSON(map[string]any{
			"connection":     session.name,
			"dataId":         dataID,
			"group":          group,
			"namespace":      namespace,
			"totalCount":     page.TotalCount,
			"pageNumber":     page.PageNumber,
			"pagesAvailable": page.PagesAvailable,
			"revisions":      revisions,
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

	case "list_instances":
		serviceName := stringArg(arguments, "serviceName")
		if serviceName == "" {
			return toolError("serviceName is required")
		}
		namespace := stringArg(arguments, "namespace")
		if namespace == "" {
			namespace = session.namespace
		}
		groupName := stringArg(arguments, "groupName")
		if groupName == "" {
			groupName = defaultGroup
		}
		page, err := session.client.listInstances(ctx, namespace, groupName, serviceName,
			stringArg(arguments, "cluster"))
		if err != nil {
			return toolError(err.Error())
		}
		instances := make([]map[string]any, 0, len(page.Hosts))
		healthyCount := 0
		for _, host := range page.Hosts {
			if host.Healthy {
				healthyCount++
			}
			entry := map[string]any{
				"ip":      host.IP,
				"port":    host.Port,
				"healthy": host.Healthy,
				"enabled": host.Enabled,
				"weight":  host.Weight,
				"cluster": host.ClusterName,
			}
			// valid 只在服务端提供时输出（v1 响应有该字段，v3 的 Instance 没有）。
			if host.Valid != nil {
				entry["valid"] = *host.Valid
			}
			if len(host.Metadata) > 0 {
				entry["metadata"] = host.Metadata
			}
			instances = append(instances, entry)
		}
		return toolJSON(map[string]any{
			"connection":    session.name,
			"namespace":     namespace,
			"serviceName":   serviceName,
			"groupName":     groupName,
			"instanceCount": len(page.Hosts),
			"healthyCount":  healthyCount,
			"instances":     instances,
		}, false)

	case "find_in_configs":
		keyword := stringArg(arguments, "keyword")
		if keyword == "" {
			return toolError("keyword is required")
		}
		namespace := stringArg(arguments, "namespace")
		if namespace == "" {
			namespace = session.namespace
		}
		result, err := p.findConfigContent(ctx, session, namespace,
			stringArg(arguments, "dataId"), stringArg(arguments, "group"), keyword,
			intArg(arguments, "maxResults"), intArg(arguments, "scanLimit"), boolArg(arguments, "caseSensitive"))
		if err != nil {
			return toolError(err.Error())
		}
		return toolJSON(result, false)

	case "diff_envs":
		namespaces := stringArray(arguments, "namespaces")
		if len(namespaces) < 2 {
			return toolError("namespaces requires at least two entries; the first one is the baseline")
		}
		if len(namespaces) > 8 {
			return toolError("at most 8 namespaces per diff call")
		}
		result, err := p.diffEnvironments(ctx, session, namespaces,
			stringArg(arguments, "dataId"), stringArg(arguments, "group"),
			boolArg(arguments, "includeContentDiff"),
			intArg(arguments, "maxConfigsPerNamespace"), intArg(arguments, "maxDifferences"))
		if err != nil {
			return toolError(err.Error())
		}
		return toolJSON(result, false)

	case "diff_history":
		dataID := stringArg(arguments, "dataId")
		if dataID == "" {
			return toolError("dataId is required")
		}
		fromHistoryID := stringArg(arguments, "fromHistoryId")
		if fromHistoryID == "" {
			return toolError("fromHistoryId is required")
		}
		group := stringArg(arguments, "group")
		if group == "" {
			group = defaultGroup
		}
		namespace := stringArg(arguments, "namespace")
		if namespace == "" {
			namespace = session.namespace
		}
		result, err := p.diffConfigHistory(ctx, session, namespace, dataID, group,
			fromHistoryID, stringArg(arguments, "toHistoryId"), intArg(arguments, "contextLines"))
		if err != nil {
			return toolError(err.Error())
		}
		return toolJSON(result, false)

	case "diagnose":
		return toolJSON(p.diagnoseConnection(ctx, session), false)

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

// cachedSession is a connectionSession plus the fingerprint of the resolved
// connection it was built from, so a stale entry is never reused after any
// connection field (host, namespace, credentials, …) changes.
type cachedSession struct {
	session *connectionSession
	key     string
}

// sessionFor resolves a connection payload (from `lifecycle.connection`) into
// a usable client. Values missing from the payload fall back to the snapshot
// captured at connection/connect time. The resolved client is cached per
// connection id so consecutive tool calls share one token and one HTTP
// connection pool instead of logging in to Nacos on every single call.
func (p *plugin) sessionFor(connection map[string]any) (*connectionSession, error) {
	connectionID := stringArg(connection, "id")

	p.mu.Lock()
	cached, hasCached := p.connections[connectionID]
	p.mu.Unlock()

	record := connectionRecord{
		host:        connectionField(connection, "host"),
		port:        connectionPort(connection),
		consolePort: connectionIntField(connection, "console_port"),
		contextPath: connectionField(connection, "context_path"),
		namespace:   connectionField(connection, "namespace"),
		apiVersion:  connectionField(connection, "api_version"),
		username:    connectionField(connection, "username"),
		password:    connectionField(connection, "password"),
		name:        stringArg(connection, "name"),
	}
	if hasCached {
		if record.host == "" {
			record.host = cached.host
		}
		if record.port == 0 {
			record.port = cached.port
		}
		if record.consolePort == 0 {
			record.consolePort = cached.consolePort
		}
		if record.contextPath == "" {
			record.contextPath = cached.contextPath
		}
		if record.namespace == "" {
			record.namespace = cached.namespace
		}
		if record.apiVersion == "" {
			record.apiVersion = cached.apiVersion
		}
		if record.username == "" {
			record.username = cached.username
		}
		if record.password == "" {
			record.password = cached.password
		}
		if record.name == "" {
			record.name = cached.name
		}
	}
	if record.host == "" {
		return nil, fmt.Errorf("no usable Nacos connection: open (or re-open) a saved Nacos connection in DBX and pass its connectionId")
	}
	key := sessionKey(record)

	p.mu.Lock()
	if entry, ok := p.sessions[connectionID]; ok && entry.key == key {
		session := entry.session
		p.mu.Unlock()
		return session, nil
	}
	p.mu.Unlock()

	client, err := newNacosClient(clientOptions{
		host:        record.host,
		port:        record.port,
		contextPath: record.contextPath,
		consolePort: record.consolePort,
		apiVersion:  record.apiVersion,
		username:    record.username,
		password:    record.password,
	})
	if err != nil {
		return nil, err
	}
	if record.name == "" {
		record.name = "Nacos"
	}
	session := &connectionSession{client: client, namespace: record.namespace, name: record.name}

	p.mu.Lock()
	if p.sessions == nil {
		p.sessions = map[string]cachedSession{}
	}
	if len(p.sessions) >= sessionCacheLimit {
		p.sessions = map[string]cachedSession{}
	}
	p.sessions[connectionID] = cachedSession{session: session, key: key}
	p.mu.Unlock()
	return session, nil
}

// sessionKey fingerprints a resolved connection. It is a hash so the cache key
// never holds a second plaintext copy of the password.
func sessionKey(record connectionRecord) string {
	sum := sha256.Sum256([]byte(strings.Join([]string{
		record.host,
		strconv.Itoa(record.port),
		strconv.Itoa(record.consolePort),
		record.contextPath,
		record.namespace,
		record.apiVersion,
		record.username,
		record.password,
	}, "\x00")))
	return hex.EncodeToString(sum[:])
}

// --- helpers ----------------------------------------------------------------

func toolJSON(payload any, isError bool) map[string]any {
	encoded, err := json.MarshalIndent(payload, "", "  ")
	if err != nil {
		return toolError("encoding result failed: " + err.Error())
	}
	if len(encoded) > toolResultMaxBytes {
		return toolError(fmt.Sprintf(
			"result is %d bytes, above the %d byte tool limit; narrow the query (fetch one dataId instead of a whole namespace)",
			len(encoded), toolResultMaxBytes))
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

// stringArray 从参数中提取字符串数组（逐项去除首尾空白并过滤空串）。
func stringArray(values map[string]any, key string) []string {
	raw, ok := values[key].([]any)
	if !ok {
		return nil
	}
	out := make([]string, 0, len(raw))
	for _, item := range raw {
		if value, ok := item.(string); ok && strings.TrimSpace(value) != "" {
			out = append(out, strings.TrimSpace(value))
		}
	}
	return out
}

// formatMillis 把毫秒时间戳转成本地时区的 RFC3339 字符串；非法值返回空串。
func formatMillis(millis int64) string {
	if millis <= 0 {
		return ""
	}
	return time.UnixMilli(millis).Local().Format(time.RFC3339)
}

func boolArg(values map[string]any, key string) bool {
	switch value := values[key].(type) {
	case bool:
		return value
	case string:
		return strings.EqualFold(strings.TrimSpace(value), "true")
	}
	return false
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
	return connectionIntField(connection, "port")
}

// connectionIntField 读取连接载荷中的整数配置，先查顶层字段，再回退到
// `binding: "config"` 字段所在的 external_config 映射。
func connectionIntField(connection map[string]any, key string) int {
	if value := intArg(connection, key); value > 0 {
		return value
	}
	if ext := mapValue(connection["external_config"]); len(ext) > 0 {
		return intArg(ext, key)
	}
	return 0
}
