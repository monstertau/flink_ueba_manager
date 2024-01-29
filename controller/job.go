package controller

import (
	"flink_ueba_manager/manager"
	"github.com/gin-gonic/gin"
	"net/http"
)

type JobHandler struct {
	JobManager *manager.JobManager
}

func NewJobHandler(jobManager *manager.JobManager) *JobHandler {
	return &JobHandler{
		JobManager: jobManager,
	}
}

func (s *JobHandler) MakeHandler(g *gin.RouterGroup) {
	group := g.Group("/jobs")
	group.GET("", s.getAllJobs)
	group.GET("/succeed", s.getAllJobs)
	group.GET("/failed", s.getAllJobs)
}

func (s *JobHandler) getAllJobs(c *gin.Context) {
	c.Status(http.StatusOK)
}
