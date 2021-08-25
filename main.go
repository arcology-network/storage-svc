package main

import (
	"os"

	"github.com/arcology/common-lib/extl/cli"
	"github.com/arcology/storage-svc/node"
)

func main() {

	st := node.StartCmd

	cmd := cli.PrepareMainCmd(st, "BC", os.ExpandEnv("$HOME/monacos/storage"))
	if err := cmd.Execute(); err != nil {
		os.Exit(1)
	}
}
