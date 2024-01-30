package worker

import (
	"flink_ueba_manager/config"
	"flink_ueba_manager/external"
	"flink_ueba_manager/sql_builder"
	"flink_ueba_manager/view"
	"fmt"
	"time"
)

type (
	RuleJobWorker struct {
		ID         string
		cfg        *view.RuleJobConfig
		flinkJobID string
	}
)

func NewRuleJobWorker(ID string, cfg *view.RuleJobConfig) *RuleJobWorker {
	return &RuleJobWorker{
		ID:  ID,
		cfg: cfg,
	}
}

func (s *RuleJobWorker) Stop() error {
	return nil
}

func (s *RuleJobWorker) Run() error {
	err := s.createProfilePredictorSource()
	if err != nil {
		return err
	}
	err = s.createRule()
	if err != nil {
		return err
	}
	err = s.createRuleSink()
	if err != nil {
		return err
	}
	err = s.setName()
	if err != nil {
		return err
	}
	jobID, err := s.createJob()
	if err != nil {
		return err
	}
	s.flinkJobID = jobID
	fmt.Println("Done creating job")
	return nil
}

func (s *RuleJobWorker) createProfilePredictorSource() error {
	id := getProfilePredictorIDFrom(s.cfg.ID)
	tableBuilder := sql_builder.NewTableSQLBuilder(id)
	// build schema
	schemaBuilder := sql_builder.NewSchemaSQLBuilder()
	for _, v := range getOrderedSchema(s.cfg.ProfilePredictorOutput.Schema) {
		schemaBuilder.WithColumn(v[0], v[1])
	}
	//schemaBuilder.WithColumn(timestampField, data_type.TIMESTAMP_PRECISION(3))
	//schemaBuilder.WithTimestampField(s.cfg.LogSourceConfig.TimestampField, timestampField)
	// build connector
	connectorBuilder := sql_builder.NewConnectorBuilder()
	connectorBuilder.
		WithOption("connector", "kafka").
		WithOption("topic", s.cfg.ProfilePredictorOutput.Topic).
		WithOption("properties.bootstrap.servers", s.cfg.ProfilePredictorOutput.BootstrapServer).
		WithOption("properties.group.id", config.AppConfig.KafkaGroupID).
		WithOption("scan.startup.mode", "earliest-offset").
		WithOption("format", "json").
		WithOption("json.timestamp-format.standard", "ISO-8601").
		WithOption("json.ignore-parse-errors", "true").
		WithOption("json.fail-on-missing-field", "false")

	stmStr := tableBuilder.
		WithSchema(schemaBuilder.Build()).
		WithConnector(connectorBuilder.Build()).
		Build()
	fmt.Println(stmStr)
	session := external.GetFlinkSQLSession()
	stm, err := session.SubmitStatement(stmStr)
	if err != nil {
		return err
	}
	_, err = stm.GetOperationResult(0)
	if err != nil {
		return err
	}
	return nil
}

func (s *RuleJobWorker) createRule() error {
	id := getRuleIDFrom(s.cfg.ID)
	logSrcID := getProfilePredictorIDFrom(s.cfg.ID)
	viewBuilder := sql_builder.NewViewSQLBuilder(id)

	expBuilder := sql_builder.NewFilterExpSQLBuilder()
	expBuilder.
		WithFilter(s.cfg.Filter).
		WithFields("*").
		WithQueryTable(logSrcID)
	expStr := expBuilder.Build()
	stmStr := viewBuilder.
		WithExpression(expStr).
		Build()
	fmt.Println(stmStr)
	session := external.GetFlinkSQLSession()
	stm, err := session.SubmitStatement(stmStr)
	if err != nil {
		return err
	}
	_, err = stm.GetOperationResult(0)
	if err != nil {
		return err
	}
	return nil
}

func (s *RuleJobWorker) createRuleSink() error {
	id := getRuleSinkIDFrom(s.cfg.ID)
	tableBuilder := sql_builder.NewTableSQLBuilder(id)
	// build schema
	schemaBuilder := sql_builder.NewSchemaSQLBuilder()
	for _, v := range getOrderedSchema(s.cfg.ProfilePredictorOutput.Schema) {
		schemaBuilder.WithColumn(v[0], v[1])
	}
	//schemaBuilder.WithColumn(timestampField, data_type.TIMESTAMP_PRECISION(3))
	schemaBuilder.WithColumn("alert_id", "STRING")
	schemaBuilder.WithColumn("alert_time", "STRING")
	schemaBuilder.WithColumn("rule_id", "STRING")
	schemaBuilder.WithColumn("rule_name", "STRING")
	schemaBuilder.WithColumn("technique", "STRING")
	schemaBuilder.WithColumn("severity", "STRING")
	schemaBuilder.WithColumn("risk_score", "BIGINT")
	schemaBuilder.WithColumn("object", "STRING")

	connectorBuilder := sql_builder.NewConnectorBuilder()
	connectorBuilder.
		WithOption("connector", "kafka").
		WithOption("topic", s.cfg.RuleOutput.Topic).
		WithOption("properties.bootstrap.servers", s.cfg.RuleOutput.BootstrapServer).
		WithOption("properties.group.id", config.AppConfig.KafkaGroupID).
		WithOption("format", "json")

	stmStr := tableBuilder.
		WithSchema(schemaBuilder.Build()).
		WithConnector(connectorBuilder.Build()).
		Build()
	fmt.Println(stmStr)
	session := external.GetFlinkSQLSession()
	stm, err := session.SubmitStatement(stmStr)
	if err != nil {
		return err
	}
	_, err = stm.GetOperationResult(0)
	if err != nil {
		return err
	}
	return nil
}

func (s *RuleJobWorker) setName() error {
	setCfgBuilder := sql_builder.NewSetConfigSQLBuilder()
	stmStr := setCfgBuilder.WithConfig("pipeline.name", fmt.Sprintf("rule_%v", s.cfg.ID)).Build()
	fmt.Println(stmStr)
	session := external.GetFlinkSQLSession()
	stm, err := session.SubmitStatement(stmStr)
	if err != nil {
		return err
	}
	_, err = stm.GetOperationResult(0)
	if err != nil {
		return err
	}
	return nil
}

func (s *RuleJobWorker) createJob() (string, error) {
	ruleSinkID := getRuleSinkIDFrom(s.cfg.ID)
	ruleID := getRuleIDFrom(s.cfg.ID)
	ruleExpBuilder := sql_builder.NewSelectSQLBuilder()
	bhvExpStr := ruleExpBuilder.
		WithFields(fmt.Sprintf("*,UUID(),'%v','%v',"+
			"'%v','%v','%v',%v,%v as object", time.Now().Format("2006-01-02 15:04:05"), ruleID, s.cfg.Name, s.cfg.Technique, s.cfg.Severity, s.cfg.RiskScore, s.cfg.Object)).
		WithQueryTable(ruleID).
		Build()
	bhvInsertBuilder := sql_builder.NewInsertSQLBuilder()
	insertBhvStm := bhvInsertBuilder.
		WithDestinationTable(ruleSinkID).
		WithExpression(bhvExpStr).
		Build()

	stmSetBuilder := sql_builder.NewStatementSetSQLBuilder()
	stmStr := stmSetBuilder.
		WithInsertStatement(insertBhvStm).
		Build()
	session := external.GetFlinkSQLSession()
	stm, err := session.SubmitStatement(stmStr)
	if err != nil {
		return "", err
	}
	fmt.Println(stmStr)
	opRes, err := stm.GetOperationResult(0)
	if err != nil {
		return "", err
	}
	return opRes.JobID, nil
}

func getRuleSinkIDFrom(ID string) string {
	return "rule_sink_" + ID
}

func getRuleIDFrom(ID string) string {
	return "rule_" + ID
}

func getProfilePredictorIDFrom(ID string) string {
	return "profiling_predictor_" + ID
}
