package main

import (
	"log"
	"os"

	"net/http"
	_ "net/http/pprof"

	tmCli "github.com/arcology-network/3rd-party/tm/cli"
	"github.com/arcology-network/storage-svc/service"
)

func main() {
	go func() {
		log.Println(http.ListenAndServe("localhost:6060", nil))
	}()

	st := service.StartCmd

	cmd := tmCli.PrepareMainCmd(st, "BC", os.ExpandEnv("$HOME/monacos/storage"))
	if err := cmd.Execute(); err != nil {
		os.Exit(1)
	}
}
