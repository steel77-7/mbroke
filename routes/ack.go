package routes

import (
	"log"

	"github.com/gin-gonic/gin"
	"github.com/mbroke/utils"
)

type Ack_request struct {
	ID     string
	Job_id string
	Status bool
}

func Ack(c *gin.Context) {
	log.SetPrefix("In the Ack: ")
	log.SetFlags(0)
	var req Ack_request
	if err := c.ShouldBind(&req); err != nil {
		c.JSON(
			500,
			gin.H{
				"message": "Couldnt parse the message",
			},
		)
		log.Fatal("Couldnt parse the request")
	}
	if !req.Status {
		delete(utils.Worker_map.List, req.ID)
		c.JSON(200, gin.H{
			"message": "NACK sent",
		})
		return
	}

}
