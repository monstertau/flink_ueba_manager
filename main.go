package main

import (
	"flink_ueba_manager/config"
	"flink_ueba_manager/controller"
	"flink_ueba_manager/external"
	"flink_ueba_manager/manager"
	"fmt"
	"github.com/gin-gonic/gin"
	"log"
)

func main() {
	log.Println("+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-")
	appConfig, err := config.LoadFile(config.DefaultConfigFilePath)
	if err != nil {
		log.Fatalf("failed to parse configuration file %s: %v", config.DefaultConfigFilePath, err)
	}
	config.AppConfig = appConfig

	ss, err := external.NewFlinkSQLGatewaySession()
	if err != nil {
		log.Fatalf("error in init flink SQL Gateway Session: %v", err)
	}
	external.SetFlinkSQLSession(ss)

	jobManager := manager.NewJobManager()
	jobManager.Run()

	route := gin.Default()
	apiGroup := route.Group("/api/v1")
	jobHandler := controller.NewJobHandler(jobManager)
	jobHandler.MakeHandler(apiGroup)

	err = route.Run(fmt.Sprintf("%s:%d", config.AppConfig.Service.Host, config.AppConfig.Service.Port))
	if err != nil {
		log.Fatalf("failed to start the server: %v", err)
	}
}
