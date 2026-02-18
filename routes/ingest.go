package routes

import (
	"encoding/json"
	"log"

	"github.com/gin-gonic/gin"
	"github.com/mbroke/types"
	"github.com/mbroke/utils"
)

func Ingest(c *gin.Context) {
	defer c.Request.Body.Close()
	job := types.Job{}
	req := types.Job_req{}
	if err := c.ShouldBindJSON(&req); err != nil {
		log.Print("Couldn't bind the json: ", err)
		c.JSON(500, gin.H{
			"message": "Couldn't bidn the json: ",
			"code":    500,
		})
		return
	}
	job.Metadata = string(req.Metadata)
	job.Data = string(req.Data)
	//	utils.Ingest_channel <- job
	var meta types.Metadata
	err := json.Unmarshal(req.Metadata, &meta)
	if err != nil {
		log.Println("failed to parse metadata:", err)
		return
	}
	err1 := utils.Add_into_dict(meta)
	if err1 != nil {
		log.Print("couldnt add to the dict", err)
		return
	}
	utils.Feed(job)
	c.JSON(201, gin.H{
		"message": "job added to the queue",
		"code":    201,
	})
}
