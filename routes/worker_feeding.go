package routes

import (
	"io"
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
	//	tbs, _ = string(json.Marshal(job))
	var req_bytes worker_info
	jsonData, err := io.ReadAll(c.Request.Body)
	if err != nil {
		c.AbortWithStatus(400)
		return
	}
	log.Println("Joined worker", string(jsonData))
	if err := c.ShouldBind(&req_bytes); err != nil {
		log.Print("Couldn't get the worker id in the [Worker feeding")
		c.JSON(500, gin.H{
			"message": "No id provided",
			"status":  500,
			"error":   err,
		})
		return
	}
	//	job.Worker = req_bytes.ID

	var job types.Job
	if len(utils.Worker_channel) > 0 {

		job = <-utils.Worker_channel
	}

	check, _ := utils.Redis.XRange(utils.CTX, "ingest:primary", job.ID, job.ID).Result()

	if len(check) != 0 {
		_, err := utils.Redis.XClaim(utils.CTX, &redis.XClaimArgs{
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
	}
	// } else {
	// 	args := &redis.XAddArgs{
	// 		Stream: "ingest:primary",
	// 		// Group:    "primary",
	// 		// Consumer: req_bytes.ID,
	// 		MaxLen: 20000,
	// 		Values: job,
	// 	}
	// 	_, err := utils.Redis.XAdd(utils.CTX, args).Result()
	// 	if err != nil {
	// 		c.JSON(500, gin.H{
	// 			"message": "Job not added",
	// 		})
	// 		log.Fatal("Couldnt add the job in Worker feeding", err)
	// 	}
	// }

	// issue here : the job and worker id issue
	utils.Worker_map.List[req_bytes.ID] = &types.Worker{
		ID:        req_bytes.ID,
		Job_id:    job.ID,
		Last_ping: time.Now().UTC().UnixMilli(),
	}
	//utils.Feed()
	log.Print(job.Data)
	c.JSON(200, gin.H{
		"message": "Job retrieved",
		"data":    job.Data,
	})
}
