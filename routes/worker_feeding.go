package routes

import (
	"encoding/json"
	"log"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/mbroke/types"
	"github.com/mbroke/utils"
	"github.com/redis/go-redis/v9"
)

type worker_info struct {
	ID string `json:"id"`
}

func Worker_feeding(c *gin.Context) {
	var job types.Job = <-utils.Worker_channel
	tbs, _ = string(json.Marshal(job))
	var req_bytes worker_info
	if err := c.ShouldBind(&req_bytes); err == nil {
		log.Print("Couldn't get the worker id in the [Worker feeding")
		c.JSON(500, gin.H{
			"message": "No id provided",
			"status":  500,
			"error":   err,
		})
		return
	}
	job.Worker = req_bytes.ID
	check, _ := utils.Redis.XRange(utils.CTX, job.ID, job.ID)

	if len(check) != 0 {

		claimed, err := utils.Redis.XCLaim(utils.CTX, &redis.XClaimArgs{
			Stream:   "ingest:primary",
			Group:    "primary",
			Consumer: req_bytes.ID,
			Messages: []string{job.ID},
		}).Result()
		if err != nil {
			c.JSON(500, gin.H{
				"message": "Job not retrieved",
			})
			log.Fatal("Couldnt claim the job in Worker feeding")

		}
	} else {
		args := &redis.XAddArgs{
			Stream: "ingest:primary",
			// Group:    "primary",
			// Consumer: req_bytes.ID,
			MaxLen: 20000,
			Values: job,
		}
		res, err := utils.Redis.XAdd(utils.CTX, args)
		if err != nil {
			c.JSON(500, gin.H{
				"message": "Job not added",
			})
			log.Fatal("Couldnt add the job in Worker feeding")
		}
	}

	utils.Worker_map.Mu.Lock()
	// issue here : the job and worker id issue
	utils.Worker_map.List[req_bytes.ID] = &types.Worker{
		ID:        req_bytes.ID,
		Job_id:    job.ID,
		Last_ping: time.Now().UTC().UnixMilli(),
	}
	utils.Worker_map.Mu.Unlock()
	//utils.Feed()
	c.JSON(200, gin.H{
		"message": "Job retrieved",
	})
}
