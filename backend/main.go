package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"

	dbxpluginsdk "github.com/t8y2/dbx/plugins/sdk/go/dbx-plugin-sdk"
)

const (
	pluginID      = "io.github.ct17.nacos_toolkit"
	pluginVersion = "0.3.0"
)

// connectionRecord is the normalized connection payload captured at
// connection/connect time. Tool calls prefer the lifecycle payload the host
// injects; the record is a fallback when fields are absent.
type connectionRecord struct {
	host        string
	port        int
	consolePort int // Nacos 3.x 控制台端口（0 表示未配置，探测时按默认 8080 尝试）
	contextPath string
	namespace   string
	apiVersion  string // auto / v1 / v3，空按 auto 处理
	username    string
	password    string
	name        string
}

type plugin struct {
	mu          sync.Mutex
	connections map[string]connectionRecord
	// sessions caches one live Nacos client per connection so repeated tool
	// calls reuse the token and the HTTP connection pool instead of logging in
	// again on every call.
	sessions map[string]cachedSession
}

// newPlugin builds a plugin with its maps initialized.
func newPlugin() *plugin {
	return &plugin{
		connections: map[string]connectionRecord{},
		sessions:    map[string]cachedSession{},
	}
}

func (p *plugin) Handle(
	_ dbxpluginsdk.RequestContext,
	method string,
	params json.RawMessage,
	_ *dbxpluginsdk.Emitter,
) (any, *dbxpluginsdk.PluginError) {
	var values map[string]any
	if err := json.Unmarshal(params, &values); err != nil {
		return nil, dbxpluginsdk.NewError(-32602, "Invalid request parameters")
	}
	switch method {
	case "connection/test":
		return p.handleConnectionTest(values)
	case "connection/connect":
		return p.handleConnectionConnect(values)
	case "connection/disconnect":
		return p.handleConnectionDisconnect(values)
	case "nacos/ping":
		return p.handlePing(values)
	case "mcp/tools":
		return mcpTools(), nil
	case "mcp/call":
		return p.callTool(values), nil
	default:
		return nil, dbxpluginsdk.MethodNotFound(method)
	}
}

// handleConnectionTest verifies reachability and the data plane. The probe is
// deliberately the same code path tools use, so a stored "connection ok" means
// the tools will work.
func (p *plugin) handleConnectionTest(values map[string]any) (any, *dbxpluginsdk.PluginError) {
	record, err := resolveConnection(mapValue(values["connection"]))
	if err != nil {
		return nil, dbxpluginsdk.NewError(-32602, err.Error())
	}
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
		return nil, dbxpluginsdk.NewError(-32602, err.Error())
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	status, err := client.health(ctx)
	if err != nil {
		return nil, dbxpluginsdk.NewError(-32010, fmt.Sprintf("Nacos is not reachable: %v", err))
	}
	// Data-plane check: health endpoints are often open even when the config
	// API requires authentication, so probe a real config read as well.
	if client.resolvedFamilyName() == "v3" && record.username == "" {
		// Nacos 3.x 的 console 能力（搜索/历史/命名空间/服务）始终需要账号。
		// 无凭据时探测 client 读接口：可用则提示补充凭据以解锁全部工具；
		// 不可用（鉴权开启）则按数据面失败处理。
		if _, _, err := client.readConfig(ctx, record.namespace, "__connectivity_probe__", defaultGroup); err != nil {
			return nil, dbxpluginsdk.NewError(-32010,
				fmt.Sprintf("connected to %s but reading configs failed: %v", status, err))
		}
		return map[string]any{
			"success": true,
			"message": fmt.Sprintf(
				"Nacos responded: %s (API v3; console APIs need credentials — set username/password to enable search/history/namespaces/services)",
				status),
		}, nil
	}
	if _, err := client.searchConfigs(ctx, record.namespace, "", "", 1, 1); err != nil {
		return nil, dbxpluginsdk.NewError(-32010,
			fmt.Sprintf("connected to %s but reading configs failed: %v", status, err))
	}
	return map[string]any{
		"success": true,
		"message": fmt.Sprintf("Nacos responded: %s (API %s)", status, client.resolvedFamilyName()),
	}, nil
}

// handleConnectionConnect stores the connection snapshot for later use.
func (p *plugin) handleConnectionConnect(values map[string]any) (any, *dbxpluginsdk.PluginError) {
	connection := mapValue(values["connection"])
	connectionID := stringArg(connection, "id")
	if connectionID == "" {
		return nil, dbxpluginsdk.NewError(-32602, "Missing connection id")
	}
	record, err := resolveConnection(connection)
	if err != nil {
		return nil, dbxpluginsdk.NewError(-32602, err.Error())
	}
	p.mu.Lock()
	p.connections[connectionID] = record
	p.mu.Unlock()
	return map[string]any{"success": true}, nil
}

// handleConnectionDisconnect drops the stored snapshot and the cached client.
func (p *plugin) handleConnectionDisconnect(values map[string]any) (any, *dbxpluginsdk.PluginError) {
	connection := mapValue(values["connection"])
	connectionID := stringArg(connection, "id")
	if connectionID == "" {
		return nil, dbxpluginsdk.NewError(-32602, "Missing connection id")
	}
	p.mu.Lock()
	delete(p.connections, connectionID)
	delete(p.sessions, connectionID)
	p.mu.Unlock()
	// Go strings cannot be zeroed in place, so the password bytes stay on the
	// heap until the GC reclaims them; dropping both maps shortens the window.
	// The credential store itself belongs to the host, not to the plugin.
	return map[string]any{"success": true}, nil
}

// handlePing answers the workbench UI: reports upstream status for the
// connection currently open in the workbench.
func (p *plugin) handlePing(values map[string]any) (any, *dbxpluginsdk.PluginError) {
	connectionID := stringArg(values, "connectionId")
	p.mu.Lock()
	record, ok := p.connections[connectionID]
	p.mu.Unlock()
	if !ok {
		return nil, dbxpluginsdk.NewError(-32010, "Open a saved Nacos connection to check its status")
	}
	// The probe deliberately runs without credentials: both the v1 readiness
	// endpoint and the v3 console health endpoint answer anonymously, and a
	// reachability check has no reason to hand the password upstream.
	// connection/test is the path that validates credentials and the data plane.
	client, err := newNacosClient(clientOptions{
		host:        record.host,
		port:        record.port,
		contextPath: record.contextPath,
		consolePort: record.consolePort,
		apiVersion:  record.apiVersion,
	})
	if err != nil {
		return nil, dbxpluginsdk.NewError(-32010, err.Error())
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	status, err := client.health(ctx)
	if err != nil {
		return nil, dbxpluginsdk.NewError(-32010, fmt.Sprintf("Nacos is not reachable: %v", err))
	}
	return map[string]any{
		"ok":      true,
		"baseUrl": client.serverBase,
		"api":     client.resolvedFamilyName(),
		"message": status,
	}, nil
}

// resolveConnection normalizes a raw connection payload into a record.
func resolveConnection(connection map[string]any) (connectionRecord, error) {
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
	if record.host == "" {
		return record, fmt.Errorf("connection has no server address")
	}
	return record, nil
}

func main() {
	metadata := dbxpluginsdk.Metadata{
		ID:           pluginID,
		Version:      pluginVersion,
		Capabilities: []string{"connections", "mcp"},
	}
	server := dbxpluginsdk.NewServer(metadata, newPlugin())
	if err := server.Serve(); err != nil {
		log.Fatal(err)
	}
}
