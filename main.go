package main

import (
	"net/http"

	"github.com/gin-gonic/gin"
)

func main() {
	router := gin.Default()
	//	router.GET("/", home)
	router.GET("/", func(c *gin.Context) {
		c.IndentedJSON(http.StatusOK, res{Msg: "helooooo beitch"})
	})

	router.Run("localhost:8000")
}

type res struct {
	Msg string `json:"msg"`
}

func home(c *gin.Context) {
	c.IndentedJSON(http.StatusOK, res{Msg: "helooooo beitch"})
}
