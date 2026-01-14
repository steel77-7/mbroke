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
	defer c.Request.Body.Close()

	var req Ack_request
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(
			500,
			gin.H{
				"message": "Couldnt parse the message",
			},
		)
		log.Print("Couldnt parse the request")
		return
	}
	if !req.ACK {
		c.JSON(200, gin.H{
			"message": "NACK recieved",
		})
	} else {
		utils.Worker_map.Mu.Lock()
		worker := utils.Worker_map.List[req.ID]
		utils.Worker_map.Mu.Unlock()

		//err := utils.ACK(worker.Job_id)
		utils.ACK_channel <- worker.Job_id
		// if err {
		// 	c.JSON(500,
		// 		gin.H{
		// 			"message": "messafe couldnt be acked",
		// 		})
		// 	log.Print("COuldnt be acked: ", req.ID)
		// }
		c.JSON(200, gin.H{
			"message": "ACK recieved",
		})
		return
	}
	utils.Worker_map.Mu.Lock()
	delete(utils.Worker_map.List, req.ID)
	utils.Worker_map.Mu.Unlock()

}
