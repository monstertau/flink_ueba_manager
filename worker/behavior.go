package worker

import (
	"flink_ueba_manager/config"
	"flink_ueba_manager/external"
	"flink_ueba_manager/sql_builder"
	"flink_ueba_manager/sql_builder/data_type"
	"flink_ueba_manager/view"
	"fmt"
	"sort"
)

const (
	timestampField = "converted_ts"
)

type (
	IFlinkSQLWorker interface {
		Run() error
	}
	BehaviorJobWorker struct {
		ID         string
		cfg        *view.BehaviorJobConfig
		flinkJobID string
	}
)

func NewBehaviorJobWorker(ID string, cfg *view.BehaviorJobConfig) *BehaviorJobWorker {
	return &BehaviorJobWorker{
		ID:  ID,
		cfg: cfg,
	}
}

func (s *BehaviorJobWorker) Stop() error {
	return nil
}

func (s *BehaviorJobWorker) Run() error {
	err := s.createLogSource()
	if err != nil {
		return err
	}
	err = s.createBehavior()
	if err != nil {
		return err
	}
	err = s.createProfileBatch()
	if err != nil {
		return err
	}
	err = s.createBehaviorSink()
	if err != nil {
		return err
	}
	err = s.createProfilingSink()
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

func (s *BehaviorJobWorker) createLogSource() error {
	id := getLogSourceIDFrom(s.cfg.ID)
	tableBuilder := sql_builder.NewTableSQLBuilder(id)
	// build schema
	schemaBuilder := sql_builder.NewSchemaSQLBuilder()
	for _, v := range getOrderedSchema(s.cfg.LogSourceConfig.Schema) {
		schemaBuilder.WithColumn(v[0], v[1])
	}
	schemaBuilder.WithTimestampField(s.cfg.LogSourceConfig.TimestampField, timestampField)
	// build connector
	connectorBuilder := sql_builder.NewConnectorBuilder()
	connectorBuilder.
		WithOption("connector", "kafka").
		WithOption("topic", s.cfg.LogSourceConfig.Topic).
		WithOption("properties.bootstrap.servers", s.cfg.LogSourceConfig.BootstrapServer).
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

func (s *BehaviorJobWorker) createBehavior() error {
	id := getBehaviorIDFrom(s.cfg.ID)
	logSrcID := getLogSourceIDFrom(s.cfg.ID)
	viewBuilder := sql_builder.NewViewSQLBuilder(id)

	expBuilder := sql_builder.NewFilterExpSQLBuilder()
	expBuilder.
		WithFilter(s.cfg.BehaviorFilter).
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

func (s *BehaviorJobWorker) createProfileBatch() error {
	id := getProfileIDFrom(s.cfg.ID)
	bhvID := getBehaviorIDFrom(s.cfg.ID)
	viewBuilder := sql_builder.NewViewSQLBuilder(id)
	var (
		entities   []string
		attributes []string
	)
	for _, ent := range s.cfg.ProfileConfig.Entities {
		entities = append(entities, ent.Name)
	}
	for _, att := range s.cfg.ProfileConfig.Attributes {
		attributes = append(attributes, att.Name)
	}
	expBuilder := sql_builder.NewTumblingCntWindowExpSQLBuilder()

	expStr := expBuilder.
		WithQueryTable(bhvID).
		WithEntities(entities).
		WithAttributes(attributes).
		WithTimestampField(timestampField).
		WithMinuteInterval(s.cfg.ProfileConfig.SavingDuration).
		Build()

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

func (s *BehaviorJobWorker) createBehaviorSink() error {
	id := getBehaviorSinkIDFrom(s.cfg.ID)
	tableBuilder := sql_builder.NewTableSQLBuilder(id)
	// build schema
	schemaBuilder := sql_builder.NewSchemaSQLBuilder()
	for _, v := range getOrderedSchema(s.cfg.LogSourceConfig.Schema) {
		schemaBuilder.WithColumn(v[0], v[1])
	}
	schemaBuilder.WithColumn(timestampField, data_type.TIMESTAMP_PRECISION(3))

	connectorBuilder := sql_builder.NewConnectorBuilder()
	connectorBuilder.
		WithOption("connector", "kafka").
		WithOption("topic", s.cfg.BehaviorOutput.Topic).
		WithOption("properties.bootstrap.servers", s.cfg.BehaviorOutput.BootstrapServer).
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

func (s *BehaviorJobWorker) createProfilingSink() error {
	id := getProfilingSinkIDFrom(s.cfg.ID)
	tableBuilder := sql_builder.NewTableSQLBuilder(id)
	// build schema
	schemaBuilder := sql_builder.NewSchemaSQLBuilder()
	schemaBuilder.
		WithColumn("window_start", data_type.TIMESTAMP_PRECISION(3)).
		WithColumn("window_end", data_type.TIMESTAMP_PRECISION(3)).
		WithColumn("cnt", data_type.BIGINT()).
		WithColumn("entities", data_type.STRING()).
		WithColumn("attributes", data_type.STRING())

	connectorBuilder := sql_builder.NewConnectorBuilder()
	connectorBuilder.
		WithOption("connector", "kafka").
		WithOption("topic", s.cfg.ProfileOutput.Topic).
		WithOption("properties.bootstrap.servers", s.cfg.ProfileOutput.BootstrapServer).
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

func (s *BehaviorJobWorker) setName() error {
	setCfgBuilder := sql_builder.NewSetConfigSQLBuilder()
	stmStr := setCfgBuilder.WithConfig("pipeline.name", fmt.Sprintf("behavior_%v", s.cfg.ID)).Build()
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

func (s *BehaviorJobWorker) createJob() (string, error) {
	profilingSinkID := getProfilingSinkIDFrom(s.cfg.ID)
	profileID := getProfileIDFrom(s.cfg.ID)
	profExpBuilder := sql_builder.NewSelectSQLBuilder()
	profExpStr := profExpBuilder.
		WithFields("*").
		WithQueryTable(profileID).
		Build()
	profInsertBuilder := sql_builder.NewInsertSQLBuilder()
	insertProfilingStm := profInsertBuilder.
		WithDestinationTable(profilingSinkID).
		WithExpression(profExpStr).
		Build()

	bhvSinkID := getBehaviorSinkIDFrom(s.cfg.ID)
	bhvID := getBehaviorIDFrom(s.cfg.ID)
	bhvExpBuilder := sql_builder.NewSelectSQLBuilder()
	bhvExpStr := bhvExpBuilder.
		WithFields("*").
		WithQueryTable(bhvID).
		Build()
	bhvInsertBuilder := sql_builder.NewInsertSQLBuilder()
	insertBhvStm := bhvInsertBuilder.
		WithDestinationTable(bhvSinkID).
		WithExpression(bhvExpStr).
		Build()

	stmSetBuilder := sql_builder.NewStatementSetSQLBuilder()
	stmStr := stmSetBuilder.
		WithInsertStatement(insertBhvStm).
		WithInsertStatement(insertProfilingStm).
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

func (s *BehaviorJobWorker) MonitorJobStatus() error {
	return nil
}

func getLogSourceIDFrom(ID string) string {
	return "source_" + ID
}

func getBehaviorIDFrom(ID string) string {
	return "behavior_" + ID
}

func getProfileIDFrom(ID string) string {
	return "profile_" + ID
}

func getBehaviorSinkIDFrom(ID string) string {
	return "behavior_sink_" + ID
}

func getProfilingSinkIDFrom(ID string) string {
	return "profiling_sink_" + ID
}

func getOrderedSchema(schema map[string]string) [][]string {
	var arr [][]string
	for k, v := range schema {
		kv := make([]string, 0)
		kv = append(kv, k)
		kv = append(kv, v)
		arr = append(arr, kv)
	}
	sort.Slice(arr, func(i, j int) bool {
		return arr[i][0] < arr[j][0]
	})
	return arr
}
