package routes

import (
	"github.com/mbroke/types"
	"github.com/mbroke/utils"
	"github.com/gin-gonic/gin"
	"time"
)

func heartbeat(c *gin.Context) {
	var heartbeat types.Heartbeat
	if err := c.ShouldBind(&heartbeat); err != nill{
		c.JSON(500 ,c.H{
			"message":"Couldn't bind the result"
		})
		return
	}
	//faulty go but written as psuedo code
	val,ok := utils.Worker_map.list[heartbeat.ID]
	if !ok{
		log.Print("Invalid worker id in heartbeat endpoint" )
		c.JSON(404, c.H{
			"message":"Invalid id"
		})
		return
	}
	val.Last_ping = time.Now().UTC().UnixMilli()
	c.JSON(200,c.H{})
}
