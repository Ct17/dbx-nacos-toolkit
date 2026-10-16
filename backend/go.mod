module dbx-nacos-toolkit

go 1.22

require github.com/t8y2/dbx/plugins/sdk/go/dbx-plugin-sdk v0.1.0

// The Go SDK has no upstream module tag yet; build against the vendored copy.
// Official builds can switch this to --sdk-root / DBX_PLUGIN_SDK_ROOT instead.
replace github.com/t8y2/dbx/plugins/sdk/go/dbx-plugin-sdk => ../third_party/dbx-plugin-sdk
