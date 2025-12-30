package routes

import (
	"log"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/mbroke/types"
	"github.com/mbroke/utils"
)

func Heartbeat(c *gin.Context) {
	var heartbeat types.Heartbeat
	if err := c.ShouldBind(&heartbeat); err != nil {
		c.JSON(500, gin.H{
			"message": "Couldn't bind the result",
		})
		return
	}
	//faulty go but written as psuedo code
	val, ok := utils.Worker_map.List[heartbeat.ID]
	if !ok {
		log.Print("Invalid worker id in heartbeat endpoint")
		c.JSON(404, gin.H{
			"message": "Invalid id",
		})
		return
	}
	val.Last_ping = time.Now().UTC().UnixMilli()
	c.JSON(200, gin.H{
		"message": "Success",
	})
}
