package main

import (
	"context"
	"path/filepath"
	"strings"
	"time"

	"github.com/kevin-chtw/tw_common/utils"
	"github.com/pitaya/tw_tourney_svr/service"
	"github.com/pitaya/tw_tourney_svr/storage"
	"github.com/redis/go-redis/v9"
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"github.com/topfreegames/pitaya/logger"
	pitaya "github.com/topfreegames/pitaya/v3/pkg"
	"github.com/topfreegames/pitaya/v3/pkg/component"
	"github.com/topfreegames/pitaya/v3/pkg/config"
	"github.com/topfreegames/pitaya/v3/pkg/serialize"
)

var app pitaya.Pitaya

func initRedis() (*redis.Client, error) {
	client := redis.NewClient(&redis.Options{
		Addr:     viper.GetString("redis.host") + ":" + viper.GetString("redis.port"),
		Password: viper.GetString("redis.password"),
		DB:       viper.GetInt("redis.db"),
		PoolSize: viper.GetInt("redis.pool_size"),
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if _, err := client.Ping(ctx).Result(); err != nil {
		return nil, err
	}

	return client, nil
}

func main() {
	// Load config
	viper.SetConfigName("config")
	viper.SetConfigType("yaml")
	viper.AddConfigPath(filepath.Join(".", "etc/tourney"))
	if err := viper.ReadInConfig(); err != nil {
		logger.Log.Fatalf("failed to read config: %v", err)
	}

	pitaya.SetLogger(utils.Logger(logrus.DebugLevel))

	config := config.NewDefaultPitayaConfig()
	config.SerializerType = uint16(serialize.PROTOBUF)
	config.Handler.Messages.Compression = false
	builder := pitaya.NewBuilder(false, "tourney", pitaya.Cluster, map[string]string{}, *config)
	app = builder.Build()
	defer app.Shutdown()

	client, err := initRedis()
	if err != nil {
		logger.Log.Fatalf("failed to init redis: %v", err)
	}
	storage := storage.NewRedisStorage(client)
	// 注册服务
	initServices(storage)

	logger.Log.Infof("Pitaya tourney server started")
	app.Start()
}

func initServices(storage *storage.RedisStorage) {
	remote := service.NewRemote(app, storage)
	app.RegisterRemote(remote, component.WithName("remote"), component.WithNameFunc(strings.ToLower))

	player := service.NewPlayer(app, storage)
	app.Register(player, component.WithName("player"), component.WithNameFunc(strings.ToLower))
}
