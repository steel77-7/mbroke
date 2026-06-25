package main

import (
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/joho/godotenv"
	"github.com/mbroke/routes"
	"github.com/mbroke/utils"
)

func main() {
	//loading the redis and broker configuration variables
	godotenv.Load()
	utils.Conf, utils.RedConf = utils.LoadConfig()
	router := gin.New()
	router.Use(gin.Recovery())

	//starts the http endpoint for feeding the workers
	router.POST("/ingest", routes.Ingest)
	go func() {
		server := utils.NewServer(":" + fmt.Sprint(utils.Conf.TCPServerPort))
		err := server.Start()
		if err != nil {
			log.Fatal(err)
		}
	}()
	//starting the broker goroutines
	utils.Redis_init()
	utils.StartIngester()
	go utils.Acker()
	go utils.Feed_to_worker()
	go utils.Pending_jobs()

	server := &http.Server{
		Addr:           ":" + fmt.Sprint(utils.Conf.Port),
		Handler:        router,
		ReadTimeout:    5 * time.Second,
		WriteTimeout:   5 * time.Second,
		IdleTimeout:    10 * time.Second,
		MaxHeaderBytes: 1 << 20,
	}

	log.Print("Sever running")
	log.Fatal(server.ListenAndServe())
}
