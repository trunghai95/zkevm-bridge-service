package main

import (
	"fmt"
	"github.com/0xPolygonHermez/zkevm-node/log"
	"github.com/alibaba/sentinel-golang/ext/datasource"
	"github.com/alibaba/sentinel-golang/pkg/datasource/apollo"
	"github.com/apolloconfig/agollo/v4/env/config"
	"github.com/urfave/cli/v2"
	"os"
	"os/signal"
)

func testMiddleware(ctx *cli.Context) error {
	logger, _, err := log.NewLogger(log.Config{
		Environment: "development",
		Level:       "debug",
		Outputs:     []string{"stdout"},
	})
	if err != nil {
		fmt.Printf("Init logger err[%v]", err)
		return err
	}
	propertyHandler := datasource.NewDefaultPropertyHandler(
		func(src []byte) (interface{}, error) {
			return string(src), nil
		},
		func(data interface{}) error {
			s := data.(string)
			logger.Errorf("update new config: %v", s)
			return nil
		})

	cfg := &config.AppConfig{
		AppID:          "haitest",
		Cluster:        "default",
		IP:             "http://apollo-server-dev.okg.com/",
		NamespaceName:  "application",
		IsBackupConfig: true,
		Secret:         "21270437515a4b66b45b9cecc1932c78",
	}

	logger.Debug("start initializing config")
	source, err := apollo.NewDatasource(cfg, "testkey1", apollo.WithPropertyHandlers(propertyHandler), apollo.WithLogger(logger))
	if err != nil {
		logger.Errorf("NewDataSource err[%v]", err)
		return err
	}
	err = source.Initialize()
	if err != nil {
		logger.Errorf("datasource Initialize err[%v]", err)
		return err
	}
	logger.Debug("finish initializing config")

	// Wait for an in interrupt.
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, os.Interrupt)
	<-ch
	return nil
}
