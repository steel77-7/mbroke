package routes

import (
	"log"
	"github.com/redis/go-redis/v9"
	"github.com/gin-gonic/gin"
	"github.com/mbroke/utils"
)
func Ingest(c *gin.Context) {
	log.SetPrefix("Error in ingest: ")
	log.SetFlags(0)
	job := Job{}
	if err := c.ShouldBind(&job) ; err!=nil {
		log.Fatal("Couldn't bind the json")
		c.JSON(500 ,c.H{
			"message":"Couldn't bidn the json: "+err,
			"code":500
		})
		return
	}
	utils.Ingest_channel<-job
	c.JSON(201,c.H{
		"message":"job added to the queue"
		"code": 201
	})
}
