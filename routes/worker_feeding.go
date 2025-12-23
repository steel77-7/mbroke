package routes

import(
	"log"
	"github.com/mbroke/types"
	"github.com/mbroke/utils"
	"github.com/gin-gonic/gin"
	"encoding/json"
	"time"
)

type worker_info struct{
	id string `json:"id"`
}
func Worker_feeding(c *gin.Context){
	var job types.Job = <-utils.Worker_channel
	tbs , _ = string(json.Marshal(job))
	var req_bytes worker_info
	if err:= c.ShouldBind(&req_bytes);err == nil{
		log.Print("Couldn't get the worker id in the [Worker feeding")
		c.JSON(500 , c.H{
			"message": "No id provided" ,
			"status": 500 ,
			"error":err,
		})
		return
	}

	Worker_map.mu.Lock()
	//issue here : the job and worker id issue
	Worker_map.list[req_bytes["id"]] = &types.Worker{
		ID:req_bytes["id"] ,
		Job_id :job.ID
		Last_ping :time.Now().UTC().UnixMilli()
	}
	Worker_map.mu.UnLock()

	c.JSON(200 ,c.H {
		"message": "Job retrieved"
		"data":job.Values["data"]
	})
}
