package routes

import (
	"encoding/json"
	"log"

	"github.com/gin-gonic/gin"
	"github.com/mbroke/types"
	"github.com/mbroke/utils"
)

type job_req struct {
	ID   string          `json:"id"`
	Data json.RawMessage `json:"data"`
}

func Ingest(c *gin.Context) {
	job := types.Job{}
	req := job_req{}
	//	raw, _ := c.GetRawData()
	//	log.Print("Raw :::::::::::", string(raw))
	log.Print("INGEST{{{{{{{}}}}}}}")
	if err := c.ShouldBindJSON(&req); err != nil {
		log.Fatal("Couldn't bind the json: ", err)
		c.JSON(500, gin.H{
			"message": "Couldn't bidn the json: ",
			"code":    500,
		})
		return
	}
	///////
	job.ID = req.ID
	job.Data = string(req.Data)
	//	utils.Ingest_channel <- job
	utils.Feed(job)
	log.Print("Job data in ingest: ", job.Data)
	c.JSON(201, gin.H{
		"message": "job added to the queue",
		"code":    201,
	})
}
