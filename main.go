package main

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/mbroke/routes"
	"github.com/mbroke/utils"
)

func main() {
	go utils.Check_heartbeat()
	router := gin.Default()
	router.GET("/", func(c *gin.Context) {
		c.IndentedJSON(http.StatusOK, res{Msg: "helooooo beitch"})
	})
	router.POST("/ingest", routes.Ingest)
	router.POST("/worker", routes.Worker_feeding)
	router.POST("/heartbeat", routes.Heartbeat)
	router.POST("/ack", routes.Ack)
	utils.Redis_init()

	router.Run("localhost:8000")
}

type res struct {
	Msg string `json:"msg"`
}

func home(c *gin.Context) {
	c.IndentedJSON(http.StatusOK, res{Msg: "helooooo beitch"})
}
