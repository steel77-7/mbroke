package routes

import (
	"encoding/json"
	//	"log"

	"github.com/mbroke/utils"

	"github.com/gin-gonic/gin"
	"github.com/mbroke/types"
)

//	func Ingest(c *gin.Context) {
//		defer c.Request.Body.Close()
//		job := types.Job{}
//		req := types.Job_req{}
//		if err := c.ShouldBindJSON(&req); err != nil {
//			log.Print("Couldn't bind the json: ", err)
//			c.JSON(500, gin.H{
//				"message": "Couldn't bidn the json: ",
//				"code":    500,
//			})
//			return
//		}
//		//log.Print("new job")
//		job.Metadata = string(req.Metadata)
//		job.Data = string(req.Data)
//		//	utils.Ingest_channel <- job
//		var meta types.Metadata
//		err := json.Unmarshal(req.Metadata, &meta)
//		if err != nil {
//			log.Println("failed to parse metadata:", err)
//			return
//		}
//		// err1 := utils.Add_into_dict(meta)
//		// if err1 != nil {
//		// 	log.Print("couldnt add to the dict", err)
//		// 	return
//		// }
//		// utils.Feed(job)
//		utils.IngesterChannel <- job
//		c.JSON(201, gin.H{
//			"message": "job added to the queue",
//			"code":    201,
//		})
//	}
func Ingest(c *gin.Context) {
	var req types.Job_req

	if err := json.NewDecoder(c.Request.Body).Decode(&req); err != nil {
		c.Status(400)
		return
	}

	utils.IngesterChannel <- types.Job{
		Metadata: string(req.Metadata),
		Data:     string(req.Data),
	}

	c.Status(201)
}
