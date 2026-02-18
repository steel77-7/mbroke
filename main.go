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

	godotenv.Load()
	utils.Conf, utils.RedConf = utils.LoadConfig()
	log.Print("redis addr", utils.Conf)

	go utils.Acker()
	go utils.Reply_to_producer()
	router := gin.New()
	router.Use(gin.Recovery())

	router.POST("/ingest", routes.Ingest)
	go func() {
		server := utils.NewServer(":" + fmt.Sprint(utils.Conf.TCPServerPort))
		err := server.Start()
		if err != nil {
			log.Fatal(err)
		}
	}()
	utils.Redis_init()
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
