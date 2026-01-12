package routes

import (
	"log"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/mbroke/types"
	"github.com/mbroke/utils"
)

type worker_info struct {
	ID string `json:"id"`
}

func Worker_feeding(c *gin.Context) {
	//	tbs, _ = string(json.Marshal(job))
	var req_bytes worker_info
	if err := c.ShouldBindJSON(&req_bytes); err != nil {
		log.Print("Couldn't get the worker id in the [Worker feeding")
		c.JSON(500, gin.H{
			"message": "No id provided",
			"status":  500,
			"error":   err,
		})
		return
	}

	job := utils.Feed_to_worker(req_bytes.ID)

	// issue here : the job and worker id issue
	utils.Worker_map.Mu.Lock()
	utils.Worker_map.List[req_bytes.ID] = &types.Worker{
		ID:        req_bytes.ID,
		Job_id:    job.ID,
		Last_ping: time.Now().UTC().UnixMilli(),
	}
	utils.Worker_map.Mu.Unlock()

	//utils.Feed()
	log.Print(len(utils.Worker_map.List))
	c.JSON(200, gin.H{
		"message": "Job retrieved",
		"data":    job.Values["data"],
	})
}
