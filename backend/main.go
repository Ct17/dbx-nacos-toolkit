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
	pluginVersion = "0.1.1"
)

// connectionRecord is the normalized connection payload captured at
// connection/connect time. Tool calls prefer the lifecycle payload the host
// injects; the record is a fallback when fields are absent.
type connectionRecord struct {
	host        string
	port        int
	contextPath string
	namespace   string
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

// handleConnectionTest verifies reachability and, when credentials are
// configured, that the credentials can actually read configs.
func (p *plugin) handleConnectionTest(values map[string]any) (any, *dbxpluginsdk.PluginError) {
	record, err := resolveConnection(mapValue(values["connection"]))
	if err != nil {
		return nil, dbxpluginsdk.NewError(-32602, err.Error())
	}
	client, err := newNacosClient(record.host, record.port, record.contextPath, record.username, record.password)
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
	if _, err := client.searchConfigs(ctx, record.namespace, "", "", 1, 1); err != nil {
		return nil, dbxpluginsdk.NewError(-32010,
			fmt.Sprintf("connected to %s but reading configs failed: %v", status, err))
	}
	return map[string]any{
		"success": true,
		"message": fmt.Sprintf("Nacos responded: %s", status),
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
	// The probe deliberately runs without credentials: Nacos serves its console
	// health endpoints anonymously, and a reachability check has no reason to
	// hand the password upstream. connection/test is the path that validates
	// credentials.
	client, err := newNacosClient(record.host, record.port, record.contextPath, "", "")
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
		"baseUrl": client.baseURL,
		"message": status,
	}, nil
}

// resolveConnection normalizes a raw connection payload into a record.
func resolveConnection(connection map[string]any) (connectionRecord, error) {
	record := connectionRecord{
		host:        connectionField(connection, "host"),
		port:        connectionPort(connection),
		contextPath: connectionField(connection, "context_path"),
		namespace:   connectionField(connection, "namespace"),
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
