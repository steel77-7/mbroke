package main

import (
	"log"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/mbroke/routes"
	"github.com/mbroke/utils"
)

func main() {
	go utils.Check_heartbeat()
	go utils.Acker()
	//	gin.SetMode(gin.ReleaseMode)
	router := gin.New()
	router.Use(gin.Recovery())
	router.GET("/", func(c *gin.Context) {
		c.IndentedJSON(http.StatusOK, res{Msg: "helooooo beitch"})
	})
	router.POST("/ingest", routes.Ingest)
	router.POST("/worker", routes.Worker_feeding)
	router.POST("/heartbeat", routes.Heartbeat)
	router.POST("/ack", routes.Ack)
	utils.Redis_init()
	server := &http.Server{
		Addr:           ":8000",
		Handler:        router,
		ReadTimeout:    5 * time.Second,
		WriteTimeout:   5 * time.Second,
		IdleTimeout:    10 * time.Second,
		MaxHeaderBytes: 1 << 20,
	}
	log.Print("Sever runnign")
	log.Fatal(server.ListenAndServe())
	// router.Run("localhost:8000")
}

type res struct {
	Msg string `json:"msg"`
}

func home(c *gin.Context) {
	c.IndentedJSON(http.StatusOK, res{Msg: "helooooo beitch"})
}
