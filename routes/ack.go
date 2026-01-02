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
	if !req.Status { //retry here
		c.JSON(200, gin.H{
			"message": "NACK recieved",
		})
	} else {
		worker := utils.Worker_map.List[req.ID]
		err := utils.ACK(worker.Job_id)
		if err {
			c.JSON(500,
				gin.H{
					"message": "messafe couldnt be acked",
				})
			log.Fatal("COuldnt be acked: ", req.ID)
		}
		c.JSON(200, gin.H{
			"message": "ACK recieved",
		})
	}
	delete(utils.Worker_map.List, req.ID)

}
