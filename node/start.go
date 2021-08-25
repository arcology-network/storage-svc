package node

import (
	"github.com/spf13/cobra"

	cmn "github.com/HPISTechnologies/common-lib/extl/common"

	"github.com/HPISTechnologies/common-lib/intl/toolkit"
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
	flags.String("query-storage", "query-storage", "topic for received storage query result")

	//common
	flags.String("msgexch", "msgexch", "topic for receive msg exchange")
	flags.String("log", "log", "topic for send log")
	flags.Int("lanes", 4, "num of scheduler lanes")
	flags.Int("svcid", 3, "index of apc in node")           //apc and state-collector need
	flags.Uint64("insid", 1, "instance id ,range 1 to 255") //apc and state-collector need

	//state-collector
	flags.String("xfer-scrs", "xfer-scrs", "topic for received state change request")
	flags.String("applylist", "applylist", "topic for received arbitrator applylist")
	flags.String("inclusive-txs", "inclusive-txs", "topic of send txlist")
	flags.String("scs-changes", "scs-changes", "topic for received smart contract result")
	flags.String("reaplist-scs", "reaplist-scs", "topic for receive reaplist of smart contract")

	flags.String("mqaddr", "localhost:9092", "host:port of kafka for update apc")

	flags.String("rank", "1", "server rank")
	flags.Int("eth-gn", 200, "init eth Account Group")
	flags.String("af", "af", "address for create genesis")

	flags.Bool("debug", false, "debug mode")

	//tcmd.AddNodeFlags(StartCmd)
}

func startCmd(cmd *cobra.Command, args []string) error {

	logger := toolkit.InitLog("storage.log")
	logger = logger.With("svc", "storage")

	en := NewService(logger)

	en.Start()

	// Wait forever
	cmn.TrapSignal(func() {
		// Cleanup
		en.Stop()
	})

	return nil
}
