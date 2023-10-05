package main

import (
	"encoding/json"
	"fmt"
	"github.com/0xPolygonHermez/zkevm-node/log"
	sentinel "github.com/alibaba/sentinel-golang/api"
	"github.com/alibaba/sentinel-golang/core/flow"
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
			ruleList := make([]*flow.Rule, 0)
			err := json.Unmarshal(src, &ruleList)
			if err != nil {
				logger.Errorf("unmarshal rule list err[%v]", err)
				return nil, err
			}
			return ruleList, nil
		},
		func(data interface{}) error {
			ruleList := data.([]*flow.Rule)
			logger.Infof("updating flow rules: %v", ruleList)
			_, err := flow.LoadRules(ruleList)
			if err != nil {
				logger.Errorf("load rules err[%v]", err)
			}
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
	source, err := apollo.NewDatasource(cfg, "flow_rules", apollo.WithPropertyHandlers(propertyHandler), apollo.WithLogger(logger))
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

	// Test using the rule
	for i := 0; i < 10; i++ {
		go func(i int) {
			cnt := 0
			for {
				e, b := sentinel.Entry("testresource")
				if b == nil {
					log.Infof("goroutine #%v req #%v printed", i, cnt)
					e.Exit()
				} else {
					//log.Errorf("goroutine #%v blocked, err[%v]", i, b.Error())
				}
			}
		}(i)
	}

	// Wait for an in interrupt.
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, os.Interrupt)
	<-ch
	return nil
}
