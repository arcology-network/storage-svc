module github.com/arcology/storage-svc

go 1.13

replace (
	github.com/arcology/common-lib => ../common-lib
	github.com/arcology/component-lib => ../component-lib
	github.com/arcology/eth-lib => ../eth-lib
	github.com/arcology/playground => ../playground
)

require (
	github.com/arcology/common-lib v0.0.0-00010101000000-000000000000
	github.com/arcology/component-lib v0.0.0-00010101000000-000000000000
	github.com/arcology/eth-lib v0.0.0-00010101000000-000000000000
	github.com/spf13/cobra v0.0.5
	github.com/spf13/viper v1.6.1
)
