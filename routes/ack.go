package routes

import (
	"log"

	"github.com/gin-gonic/gin"
	"github.com/mbroke/utils"
)

type Ack_request struct {
	ID  string `json:"id"`
	ACK bool   `json:"ack"`
}

func Ack(c *gin.Context) {
	var req Ack_request
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(
			500,
			gin.H{
				"message": "Couldnt parse the message",
			},
		)
		log.Fatal("Couldnt parse the request")
	}
	log.Print("1", req.ACK)
	if !req.ACK {
		log.Print("2")
		c.JSON(200, gin.H{
			"message": "NACK recieved",
		})
	} else {
		log.Print("3")

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
	utils.Worker_map.Mu.Lock()
	delete(utils.Worker_map.List, req.ID)
	utils.Worker_map.Mu.Unlock()

}
