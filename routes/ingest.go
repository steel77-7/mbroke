package routes

import (
	"encoding/json"

	"github.com/mbroke/utils"

	"github.com/gin-gonic/gin"
	"github.com/mbroke/types"
)

func Ingest(c *gin.Context) {
	var req types.Job_req
	if err := json.NewDecoder(c.Request.Body).Decode(&req); err != nil {
		c.Status(400)
		return
	}
	//pushing the jobs into the ingester channel that pushes the jobs into the redis queue
	utils.IngesterChannel <- types.Job{
		Metadata: string(req.Metadata),
		Data:     string(req.Data),
	}

	c.Status(201)
}
