package main

import (
	"os"

	"github.com/HPISTechnologies/common-lib/extl/cli"
	"github.com/HPISTechnologies/storage-svc/node"
)

func main() {

	st := node.StartCmd

	cmd := cli.PrepareMainCmd(st, "BC", os.ExpandEnv("$HOME/monacos/storage"))
	if err := cmd.Execute(); err != nil {
		os.Exit(1)
	}
}
