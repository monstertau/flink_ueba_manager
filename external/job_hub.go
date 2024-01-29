package external

import (
	"encoding/json"
	"flink_ueba_manager/config"
	"flink_ueba_manager/view"
	"fmt"
	"io"
	"net/http"
	"time"
)

type JobHub struct {
	behaviorEndpoint string
	ruleEndpoint     string
	timeout          time.Duration
}

func NewJobHub() *JobHub {
	return &JobHub{
		behaviorEndpoint: config.AppConfig.Endpoint.BehaviorGetJob,
		ruleEndpoint:     config.AppConfig.Endpoint.RuleGetJob,
		timeout:          1 * time.Minute,
	}
}

func (j *JobHub) GetBehaviorJobs() ([]*view.BehaviorJobConfig, error) {
	client := http.Client{
		Timeout: time.Minute,
	}
	var jobs []*view.BehaviorJobConfig
	req, err := http.NewRequest(http.MethodGet, j.behaviorEndpoint, nil)

	if err != nil {
		return nil, fmt.Errorf("in NewRequest at endpoint %s: %s", j.behaviorEndpoint, err)

	}
	req.Header.Add("Accept", "application/json")

	// make requests
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("cannot run the request at endpoint %s: %s", j.behaviorEndpoint, err)
	}
	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("cannot read the response at endpoint %s: %s", j.behaviorEndpoint, err)
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("status code: %d, response: '%s'\n", resp.StatusCode, string(respBody))
	}
	if err := json.Unmarshal(respBody, &jobs); err != nil {
		return nil, fmt.Errorf("unexpected response data at endpoint %s: %s", j.behaviorEndpoint, err)
	}
	return jobs, nil
}

func (j *JobHub) GetRuleJobs() ([]*view.RuleJobConfig, error) {
	client := http.Client{
		Timeout: time.Minute,
	}
	var jobs []*view.RuleJobConfig
	req, err := http.NewRequest(http.MethodGet, j.ruleEndpoint, nil)

	if err != nil {
		return nil, fmt.Errorf("in NewRequest at endpoint %s: %s", j.ruleEndpoint, err)

	}
	req.Header.Add("Accept", "application/json")

	// make requests
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("cannot run the request at endpoint %s: %s", j.ruleEndpoint, err)
	}
	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("cannot read the response at endpoint %s: %s", j.ruleEndpoint, err)
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("status code: %d, response: '%s'\n", resp.StatusCode, string(respBody))
	}
	if err := json.Unmarshal(respBody, &jobs); err != nil {
		return nil, fmt.Errorf("unexpected response data at endpoint %s: %s", j.ruleEndpoint, err)
	}
	return jobs, nil
}
