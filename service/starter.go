package service

import (
	tmCommon "github.com/arcology-network/3rd-party/tm/common"
	mainConfig "github.com/arcology-network/component-lib/config"
	"github.com/arcology-network/component-lib/log"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var StartCmd = &cobra.Command{
	Use:   "start",
	Short: "Start storage service Daemon",
	RunE:  startCmd,
}

func init() {
	flags := StartCmd.Flags()

	//apc
	flags.String("genesis-apc", "genesis-apc", "topic for received genesis apc")

	//common
	flags.String("msgexch", "msgexch", "topic for receive msg exchange")
	flags.String("log", "log", "topic for send log")
	flags.Int("concurrency", 4, "num of threads")

	flags.String("inclusive-txs", "inclusive-txs", "topic of send txlist")
	flags.String("euresults", "euresults", "topic for received smart contract result")
	flags.String("reaping-list", "reaping-list", "topic for receive reapinglist")

	flags.String("mqaddr", "localhost:9092", "host:port of kafka for update apc")
	flags.String("mqaddr2", "localhost:9092", "host:port of kafka for update apc")

	flags.String("af", "af", "address for create genesis")

	flags.String("receipts", "receipts", "topic for send  receipts ")

	flags.String("logcfg", "./log.toml", "log conf path")

	flags.Int("nidx", 0, "node index in cluster")
	flags.String("nname", "node1", "node name in cluster")

	flags.String("local-block", "local-block", "topic of received proposer block ")

	flags.String("zkUrl", "127.0.0.1:2181", "url of zookeeper")
	flags.String("localIp", "127.0.0.1", "local ip of server")

	flags.String("executing-logs", "executing-logs", "topic for receive executing log")
	flags.String("spawned-relations", "spawned-relations", "topic of receive spawned relations")

	flags.String("remote-caches", "", "url of remote cache cluster，default nil,use memory")

	flags.Int("logs-cache", 1000, "blocks nums of cached logs")
	flags.String("maincfg", "./monaco.toml", "main conf path")

	flags.Int("bsize", 10, "blocks nums of cached for etherscan")
	flags.Int("tsize", 10, "transactions nums of cached for etherscan")

	flags.Int("hport", 6061, "http server port")
}

func startCmd(cmd *cobra.Command, args []string) error {
	mainConfig.InitCfg(viper.GetString("maincfg"))
	log.InitLog("storage.log", viper.GetString("logcfg"), "storage", viper.GetString("nname"), viper.GetInt("nidx"))

	en := NewConfig()
	en.Start()

	// Wait forever
	tmCommon.TrapSignal(func() {
		// Cleanup
		en.Stop()
	})

	return nil
}
