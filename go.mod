module github.com/HPISTechnologies/storage-svc

go 1.13

replace (
	github.com/HPISTechnologies/common-lib => ../common-lib
	github.com/HPISTechnologies/component-lib => ../component-lib
	github.com/HPISTechnologies/eth-lib => ../eth-lib
	github.com/HPISTechnologies/playground => ../playground
)

require (
	github.com/HPISTechnologies/common-lib v0.0.0-00010101000000-000000000000
	github.com/HPISTechnologies/component-lib v0.0.0-00010101000000-000000000000
	github.com/HPISTechnologies/eth-lib v0.0.0-00010101000000-000000000000
	github.com/spf13/cobra v0.0.5
	github.com/spf13/viper v1.6.1
)
