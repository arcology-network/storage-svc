package service

import (
	"net/http"
	"strings"

	"github.com/arcology-network/component-lib/actor"
	"github.com/arcology-network/component-lib/kafka"
	"github.com/arcology-network/component-lib/storage"
	"github.com/arcology-network/component-lib/streamer"
	"github.com/arcology-network/storage-svc/service/workers"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	//"github.com/sirupsen/logrus"

	"github.com/arcology-network/component-lib/rpc"
	"github.com/spf13/viper"

	storageTypes "github.com/arcology-network/storage-svc/service/types"

	aggrv3 "github.com/arcology-network/component-lib/aggregator/v3"
	"github.com/rs/cors"
)

type Config struct {
	concurrency int
	groupid     string
}

//return a Subscriber struct
func NewConfig() *Config {
	return &Config{
		concurrency: viper.GetInt("concurrency"),
		groupid:     "storage",
	}
}

func (cfg *Config) Start() {
	http.Handle("/streamer", promhttp.Handler())
	go http.ListenAndServe(":19009", nil)

	var cachedb storageTypes.DB
	remoteUrl := viper.GetString("remote-caches")
	if remoteUrl == "" {
		cachedb = storageTypes.NewMemoryDB()
	} else {
		ips := strings.Split(remoteUrl, ",")
		cachedb = storageTypes.NewRedisDB(ips)
	}

	scanCache := storageTypes.NewScanCache(viper.GetInt("bsize"), viper.GetInt("tsize"))

	caches := storageTypes.NewLogCaches(viper.GetInt("logs-cache"))

	broker := streamer.NewStatefulStreamer()
	//00 initializer
	initializer := actor.NewActor(
		"initializer",
		broker,
		[]string{actor.MsgStarting},
		[]string{
			actor.MsgStartSub,
			actor.MsgBlockCompleted,
			actor.MsgInitParentInfo,
			actor.MsgSelectedReceipts,
			actor.MsgInitExecuted,
			actor.MsgLatestHeight,
			actor.MsgPendingBlock,
			actor.MsgExecTime,
			actor.MsgSpawnedRelations,
			actor.MsgConflictInclusive,
		},
		[]int{1, 1, 1, 1, 1, 1, 1, 1, 1, 1},
		workers.NewInitializer(cfg.concurrency, cfg.groupid, cachedb),
	)
	initializer.Connect(streamer.NewDisjunctions(initializer, 1))

	receiveMseeages := []string{
		actor.MsgBlockCompleted,
		actor.MsgParentInfo,
		actor.MsgReceipts,
		actor.MsgInclusive,
		actor.MsgPendingBlock,
		actor.MsgExecTime,
		actor.MsgSpawnedRelations,
	}

	receiveTopics := []string{
		viper.GetString("msgexch"),
		viper.GetString("receipts"),
		viper.GetString("inclusive-txs"),
		viper.GetString("local-block"),
		viper.GetString("spawned-relations"),
	}

	//01 apc module kafkaDownloader
	apcKafkaDownloader := actor.NewActor(
		"apcKafkaDownloader",
		broker,
		[]string{actor.MsgStartSub},
		receiveMseeages,
		[]int{1, 1, 1, 1, 1, 1, 1, 1},
		kafka.NewKafkaDownloader(cfg.concurrency, cfg.groupid, receiveTopics, receiveMseeages, viper.GetString("mqaddr")),
	)
	apcKafkaDownloader.Connect(streamer.NewDisjunctions(apcKafkaDownloader, 10))

	receiveMseeages2 := []string{
		actor.MsgExecutingLogs,
		actor.MsgEuResults,
	}
	receiveTopics2 := []string{
		viper.GetString("euresults"),
		viper.GetString("executing-logs"),
	}
	//01 apc module kafkaDownloader
	apcKafkaDownloaderLogs := actor.NewActor(
		"apcKafkaDownloaderLogs",
		broker,
		[]string{actor.MsgStartSub},
		receiveMseeages2,
		[]int{10, 10},
		kafka.NewKafkaDownloader(cfg.concurrency, cfg.groupid, receiveTopics2, receiveMseeages2, viper.GetString("mqaddr2")),
	)
	apcKafkaDownloaderLogs.Connect(streamer.NewDisjunctions(apcKafkaDownloaderLogs, 10))

	actor.Rename(actor.MsgInitExecuted).To(actor.MsgExecuted).On(broker)

	apc := storage.NewDBHandler(cfg.concurrency, cfg.groupid, workers.NewAPC())
	euresultBase := &actor.HeightController{}
	aggrEuresults := aggrv3.NewStatefulAggrSelector(cfg.concurrency, cfg.groupid)
	euresultBase.Next(&actor.FSMController{}).Next(actor.MakeLinkable(apc)).Next(&actor.FSMController{}).EndWith(aggrEuresults)

	chainEuresultsActor := actor.NewActor(
		"apc-aggr-selector-euresults-chain",
		broker,
		[]string{
			actor.MsgEuResults,
			actor.MsgInclusive,
			actor.MsgBlockCompleted,
			actor.MsgExecuted,
		},
		[]string{
			actor.MsgExecuted,
			actor.MsgStateData,
		},
		[]int{1, 1},
		euresultBase,
	)
	chainEuresultsActor.Connect(streamer.NewDisjunctions(chainEuresultsActor, 1))

	aggrSelectorBase := &actor.HeightController{}
	aggrSelectorBase.Next(&actor.FSMController{}).EndWith(aggrv3.NewReceiptAggreSelector(cfg.concurrency, cfg.groupid))
	//04 aggre-receipt
	aggreReceipt := actor.NewActor(
		"aggreReceipt",
		broker,
		[]string{
			actor.MsgReceipts,
			actor.MsgInclusive,
			actor.MsgBlockCompleted,
		},
		[]string{
			actor.MsgSelectedReceipts,
		},
		[]int{1},
		aggrSelectorBase,
	)
	aggreReceipt.Connect(streamer.NewDisjunctions(aggreReceipt, 1))

	//05 storage
	storages := actor.NewActor(
		"storages",
		broker,
		[]string{
			actor.MsgStateData,
			actor.MsgBlockCompleted,
			actor.MsgParentInfo,
			actor.MsgSelectedReceipts,
			actor.MsgPendingBlock,
			actor.MsgExecTime,
			actor.MsgSpawnedRelations,
			actor.MsgConflictInclusive,
		},
		[]string{
			actor.MsgLatestHeight,
		},
		[]int{1},
		workers.NewStorage(cfg.concurrency, cfg.groupid, cachedb, caches, scanCache),
	)
	storages.Connect(streamer.NewConjunctions(storages))

	rpcServerWorker := workers.NewRpcService(cfg.concurrency, cfg.groupid, cachedb, caches)

	//06 rpcServer
	rpcServer := actor.NewActor(
		"rpcServer",
		broker,
		[]string{
			actor.MsgLatestHeight,
		},
		[]string{},
		[]int{},
		rpcServerWorker,
	)
	rpcServer.Connect(streamer.NewDisjunctions(rpcServer, 1))

	//07 storage_debug
	storageDebug := actor.NewActor(
		"storageDebug",
		broker,
		[]string{
			actor.MsgExecutingLogs,
		},
		[]string{},
		[]int{},
		workers.NewStorageDebug(cfg.concurrency, cfg.groupid, cachedb),
	)
	storageDebug.Connect(streamer.NewDisjunctions(storageDebug, 1))

	relations := map[string]string{}
	relations[actor.MsgInitParentInfo] = viper.GetString("msgexch")
	relations[actor.MsgInitExecuted] = viper.GetString("genesis-apc")

	//08 kafkaUploader
	kafkaUploader := actor.NewActor(
		"kafkaUploader",
		broker,
		[]string{
			actor.MsgInitParentInfo,
			actor.MsgInitExecuted,
		},
		[]string{},
		[]int{},
		kafka.NewKafkaUploader(cfg.concurrency, cfg.groupid, relations, viper.GetString("mqaddr")),
	)
	kafkaUploader.Connect(streamer.NewDisjunctions(kafkaUploader, 1))

	actor.Rename(actor.MsgInclusive).To(actor.MsgConflictInclusive).On(broker)

	//starter
	selfStarter := streamer.NewDefaultProducer("selfStarter", []string{actor.MsgStarting}, []int{1})
	broker.RegisterProducer(selfStarter)
	broker.Serve()

	rpc.InitZookeeperRpcServer(viper.GetString("localIp")+":8974", "storage", []string{viper.GetString("zkUrl")}, []interface{}{rpcServerWorker}, nil)

	//start signel
	streamerStarting := actor.Message{
		Name:   actor.MsgStarting,
		Height: 0,
		Round:  0,
		Data:   "start",
	}
	broker.Send(actor.MsgStarting, &streamerStarting)

	c := cors.AllowAll()
	http.ListenAndServe(":"+viper.GetString("hport"), c.Handler(NewHandler(scanCache)))
}

func (*Config) Stop() {}
