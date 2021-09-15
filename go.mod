module github.com/arcology-network/storage-svc

go 1.13

replace (
	github.com/arcology-network/common-lib => ../common-lib
	github.com/arcology-network/component-lib => ../component-lib
	github.com/arcology-network/eth-lib => ../eth-lib
	github.com/arcology-network/playground => ../playground
)

require (
	github.com/arcology-network/common-lib v0.0.0-00010101000000-000000000000
	github.com/arcology-network/component-lib v0.0.0-00010101000000-000000000000
	github.com/arcology-network/eth-lib v0.0.0-00010101000000-000000000000
	github.com/spf13/cobra v0.0.5
	github.com/spf13/viper v1.6.1
)
