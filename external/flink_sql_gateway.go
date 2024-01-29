package external

import (
	"bytes"
	"encoding/json"
	"flink_ueba_manager/config"
	"flink_ueba_manager/util"
	"fmt"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"io"
	"net/http"
	"time"
)

type (
	FlinkSQLGateway struct {
		url string
	}
	FlinkSQLGatewaySession struct {
		ID      string
		gateway *FlinkSQLGateway
		logger  *logrus.Entry
	}
	FlinkSQLGatewayStatement struct {
		ID      string
		session *FlinkSQLGatewaySession
		logger  *logrus.Entry
	}
	OperationResult struct {
		ResultType string
		Result     interface{}
		JobID      string
	}
)

var _flinkSQLSession *FlinkSQLGatewaySession

func SetFlinkSQLSession(ss *FlinkSQLGatewaySession) {
	_flinkSQLSession = ss
}

func GetFlinkSQLSession() *FlinkSQLGatewaySession {
	return _flinkSQLSession
}

func NewFlinkSQLGatewaySession() (*FlinkSQLGatewaySession, error) {
	sqlGateway := NewFlinkSQLGateway()
	ss, err := sqlGateway.CreateSession()
	if err != nil {
		return nil, errors.Errorf("error in create session:%v", err)
	}
	go ss.Heartbeat()
	return ss, nil
}

func (f *FlinkSQLGatewaySession) SubmitStatement(statement string) (*FlinkSQLGatewayStatement, error) {
	endpoint := fmt.Sprintf("%v/v1/sessions/%v/statements", f.gateway.url, f.ID)
	reqBody := map[string]interface{}{
		"statement": statement,
	}
	resp, err := ExternalRequest(endpoint, http.MethodPost, reqBody)
	if err != nil {
		return nil, err
	}
	var responseMap map[string]interface{}
	if err := json.Unmarshal(resp, &responseMap); err != nil {
		return nil, fmt.Errorf("unexpected response data at endpoint %s: %s", endpoint, err)
	}
	id, ok := responseMap["operationHandle"]
	if !ok {
		return nil, fmt.Errorf("cant find operationHandle field in response of endpoint %s", endpoint)
	}
	return &FlinkSQLGatewayStatement{
		ID:      util.ParseString(id),
		session: f,
		logger:  logrus.WithField("external", "flink-sql-statement"),
	}, nil
}

func (f *FlinkSQLGatewayStatement) GetOperationResult(index int) (*OperationResult, error) {
	var (
		retryIdle = 1 * time.Second
		maxRetry  = 5
		isReady   = false
		opRes     *OperationResult
	)
outer:
	for i := 0; i < maxRetry; i++ {
		op, err := f.getOperationResult(index)
		if err != nil {
			return nil, err
		}
		if !op.IsReady() {
			time.Sleep(retryIdle)
		} else {
			opRes = op
			isReady = true
			break outer
		}
	}
	if !isReady {
		return nil, errors.Errorf("timeout while trying %v times to get operation result."+
			"SessionID: %v,OperationID: %v", maxRetry, f.session.ID, f.ID)
	}
	return opRes, nil
}

func (f *FlinkSQLGatewayStatement) getOperationResult(index int) (*OperationResult, error) {
	endpoint := fmt.Sprintf("%v/v1/sessions/%v/operations/%v/result/%v", f.session.gateway.url, f.session.ID, f.ID, index)
	resp, err := ExternalRequest(endpoint, http.MethodGet, nil)
	if err != nil {
		return nil, err
	}
	var responseMap map[string]interface{}
	if err := json.Unmarshal(resp, &responseMap); err != nil {
		return nil, fmt.Errorf("unexpected response data at endpoint %s: %s", endpoint, err)
	}
	resultType, ok := responseMap["resultType"]
	if !ok {
		return nil, fmt.Errorf("cant find resultType field in response of endpoint %s", endpoint)
	}
	op := &OperationResult{
		ResultType: util.ParseString(resultType),
	}
	if !op.IsReady() {
		return op, nil
	}

	results, ok := responseMap["results"]
	if !ok {
		return nil, fmt.Errorf("cant find resultType field in response of endpoint %s", endpoint)
	}
	op.Result = results
	jobID, ok := responseMap["jobID"]
	if ok {
		op.JobID = util.ParseString(jobID)
	}
	return op, nil
}

func (f *FlinkSQLGatewaySession) CloseOperation() error {
	return nil
}

func (f *FlinkSQLGatewaySession) Heartbeat() {

}

func (f *OperationResult) IsReady() bool {
	return f.ResultType != "NOT_READY"
}

func NewFlinkSQLGateway() *FlinkSQLGateway {
	return &FlinkSQLGateway{url: config.AppConfig.FlinkSQLGateway.URL}
}

func (f *FlinkSQLGateway) CreateSession() (*FlinkSQLGatewaySession, error) {
	endpoint := fmt.Sprintf("%v/v1/sessions", f.url)
	resp, err := ExternalRequest(endpoint, http.MethodPost, nil)
	if err != nil {
		return nil, err
	}
	var responseMap map[string]string
	if err := json.Unmarshal(resp, &responseMap); err != nil {
		return nil, fmt.Errorf("unexpected response data at endpoint %s: %s", endpoint, err)
	}
	id, ok := responseMap["sessionHandle"]
	if !ok {
		return nil, fmt.Errorf("cant find sessionHandle field in response of endpoint %s", endpoint)
	}
	return &FlinkSQLGatewaySession{
		ID:      util.ParseString(id),
		gateway: f,
		logger:  logrus.WithField("external", "flink-sql-session"),
	}, nil
}

func ExternalRequest(endpoint, method string, reqBody interface{}) ([]byte, error) {
	var b io.Reader = nil
	if data, _ := json.Marshal(reqBody); data != nil && reqBody != nil {
		b = bytes.NewBuffer(data)
	}

	client := http.Client{
		Timeout: time.Minute,
	}
	req, err := http.NewRequest(method, endpoint, b)
	if err != nil {
		return nil, err
	}
	req.Header.Add("Content-Type", "application/json")

	if err != nil {
		return nil, fmt.Errorf("in NewRequest at endpoint %s: %s", endpoint, err)

	}
	req.Header.Add("Accept", "application/json")

	// make requests
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("cannot run the request at endpoint %s: %s", endpoint, err)
	}
	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("cannot read the response at endpoint %s: %s", endpoint, err)
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("status code: %d, response: '%s'\n", resp.StatusCode, string(respBody))
	}
	return respBody, nil
}
