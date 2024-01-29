package manager

import (
	"flink_ueba_manager/external"
	"flink_ueba_manager/view"
	"flink_ueba_manager/worker"
	"fmt"
	"github.com/sirupsen/logrus"
	"time"
)

type (
	JobManager struct {
		RunningJobs map[string]*JobMetadata
		FailedJobs  map[string]*JobMetadata
		logger      *logrus.Entry
	}
	JobMetadata struct {
		worker worker.IFlinkSQLWorker
		err    error
	}
)

func NewJobManager() *JobManager {
	return &JobManager{
		RunningJobs: make(map[string]*JobMetadata),
		FailedJobs:  make(map[string]*JobMetadata),
		logger:      logrus.WithField("manager", "job"),
	}
}

func (m *JobManager) Run() {
	go func() {
		m.pullJobs()
		ticker := time.NewTicker(10 * time.Minute)
		for {
			select {
			case <-ticker.C:
				m.pullJobs()
			}
		}
	}()
}

func (m *JobManager) pullJobs() {
	jobHub := external.NewJobHub()
	bhvJobs, err := jobHub.GetBehaviorJobs()
	if err != nil {
		m.logger.Errorf("error in pulling jobs from JobHub: %v", err)
	}
	for _, job := range bhvJobs {
		id := fmt.Sprintf("behavior_%v", job.ID)
		if _, ok := m.RunningJobs[id]; ok {
			continue
		}
		delete(m.FailedJobs, id)
		err := m.CreateBehaviorJob(job)
		if err != nil {
			m.logger.Errorf("error in create behavior jobs: %v", err)
		}
	}

	ruleJobs, err := jobHub.GetRuleJobs()
	if err != nil {
		m.logger.Errorf("error in pulling jobs from JobHub: %v", err)
	}
	for _, job := range ruleJobs {
		id := fmt.Sprintf("rule_%v", job.ID)
		if _, ok := m.RunningJobs[id]; ok {
			continue
		}
		delete(m.FailedJobs, id)
		err := m.CreateRuleJob(job)
		if err != nil {
			m.logger.Errorf("error in create rule jobs: %v", err)
		}
	}
}

func (m *JobManager) CreateBehaviorJob(jobConfig *view.BehaviorJobConfig) error {
	//if _, ok := m.RunningJobs[jobConfig.ID]; ok {
	//	return fmt.Errorf("job with id %v already running", jobConfig.ID)
	//}
	bhvWorker := worker.NewBehaviorJobWorker(jobConfig.ID, jobConfig)
	err := bhvWorker.Run()
	if err != nil {
		m.FailedJobs[jobConfig.ID] = &JobMetadata{
			worker: bhvWorker,
			err:    err,
		}
		return err
	}
	m.RunningJobs[jobConfig.ID] = &JobMetadata{
		worker: bhvWorker,
	}
	return nil
}

func (m *JobManager) CreateRuleJob(jobConfig *view.RuleJobConfig) error {
	//if _, ok := m.RunningJobs[jobConfig.ID]; ok {
	//	return fmt.Errorf("job with id %v already running", jobConfig.ID)
	//}
	ruleWorker := worker.NewRuleJobWorker(jobConfig.ID, jobConfig)
	err := ruleWorker.Run()
	if err != nil {
		m.FailedJobs[jobConfig.ID] = &JobMetadata{
			worker: ruleWorker,
			err:    err,
		}
		return err
	}
	m.RunningJobs[jobConfig.ID] = &JobMetadata{
		worker: ruleWorker,
	}
	return nil
}
