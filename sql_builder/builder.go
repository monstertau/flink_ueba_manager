package sql_builder

import (
	"fmt"
)

type FlinkSQLBuilder interface {
	Build() string
}

type (
	TableSQLBuilder interface {
		FlinkSQLBuilder
		WithSchema(schema string) TableSQLBuilder
		WithConnector(connector string) TableSQLBuilder
	}
	tableSQLBuilderImpl struct {
		name      string
		schema    string
		connector string
	}
)

func NewTableSQLBuilder(tableName string) TableSQLBuilder {
	return &tableSQLBuilderImpl{
		name: tableName,
	}
}

func (t *tableSQLBuilderImpl) Build() string {
	commonStr := fmt.Sprintf("CREATE TABLE %v(%v)", t.name, t.schema)
	if t.connector != "" {
		commonStr = commonStr + fmt.Sprintf(" WITH (%v)", t.connector)
	}
	return commonStr
}

func (t *tableSQLBuilderImpl) WithSchema(schema string) TableSQLBuilder {
	t.schema = schema
	return t
}

func (t *tableSQLBuilderImpl) WithConnector(connector string) TableSQLBuilder {
	t.connector = connector
	return t
}

// SchemaSQLBuilder

type (
	SchemaSQLBuilder interface {
		FlinkSQLBuilder
		WithColumn(columnName string, columnType string) SchemaSQLBuilder
		WithTimestampField(originalTsColumn string, convertedTsColumn string) SchemaSQLBuilder
	}

	schemaSQLBuilderImpl struct {
		schemas      [][]string
		watermarkStr string
	}
)

func NewSchemaSQLBuilder() SchemaSQLBuilder {
	return &schemaSQLBuilderImpl{
		schemas: make([][]string, 0),
	}
}

func (s *schemaSQLBuilderImpl) WithColumn(columnName string, columnType string) SchemaSQLBuilder {
	col := []string{columnName, columnType}
	s.schemas = append(s.schemas, col)
	return s
}

func (s *schemaSQLBuilderImpl) WithTimestampField(originalTsColumn string, convertedTsColumn string) SchemaSQLBuilder {
	col := []string{convertedTsColumn, fmt.Sprintf("AS CAST(%v as TIMESTAMP(3))", originalTsColumn)}
	s.schemas = append(s.schemas, col)
	s.watermarkStr = fmt.Sprintf("WATERMARK for %v AS %v - INTERVAL '1' minute", convertedTsColumn, convertedTsColumn)
	return s
}

func (s *schemaSQLBuilderImpl) Build() string {
	commonStr := ""
	for _, col := range s.schemas {
		commonStr = commonStr + col[0] + " " + col[1] + ","
	}
	if s.watermarkStr != "" {
		commonStr += s.watermarkStr
	} else {
		commonStr = commonStr[:len(commonStr)-1]
	}
	return commonStr
}

// ConnectorBuilder

type (
	ConnectorBuilder interface {
		FlinkSQLBuilder
		WithOption(key, value string) ConnectorBuilder
	}
	connectorBuilderImpl struct {
		options map[string]string
	}
)

func NewConnectorBuilder() ConnectorBuilder {
	return &connectorBuilderImpl{options: make(map[string]string)}
}

func (c *connectorBuilderImpl) Build() string {
	commonStr := ""
	for column, cType := range c.options {
		commonStr = commonStr + fmt.Sprintf("'%v' = '%v'", column, cType)
		commonStr = commonStr + ","
	}
	commonStr = commonStr[:len(commonStr)-1] // delete ',' in last string
	return commonStr
}

func (c *connectorBuilderImpl) WithOption(key, value string) ConnectorBuilder {
	c.options[key] = value
	return c
}

// ViewSQLBuilder

type (
	ViewSQLBuilder interface {
		FlinkSQLBuilder
		WithExpression(expression string) ViewSQLBuilder
	}
	viewSQLBuilderImpl struct {
		name       string
		expression string
	}
)

func NewViewSQLBuilder(name string) ViewSQLBuilder {
	return &viewSQLBuilderImpl{name: name}
}

func (v *viewSQLBuilderImpl) Build() string {
	return fmt.Sprintf("CREATE VIEW %v AS %v", v.name, v.expression)
}

func (v *viewSQLBuilderImpl) WithExpression(expression string) ViewSQLBuilder {
	v.expression = expression
	return v
}

type (
	SelectExpSQLBuilder interface {
		FlinkSQLBuilder
		WithQueryTable(name string) SelectExpSQLBuilder
		WithFields(fields string) SelectExpSQLBuilder
	}
	selectExpSQLBuilderImpl struct {
		srcTableName string
		fields       string
	}
)

func NewSelectSQLBuilder() SelectExpSQLBuilder {
	return &selectExpSQLBuilderImpl{}
}

func (v *selectExpSQLBuilderImpl) WithQueryTable(name string) SelectExpSQLBuilder {
	v.srcTableName = name
	return v
}

func (v *selectExpSQLBuilderImpl) WithFields(fields string) SelectExpSQLBuilder {
	v.fields = fields
	return v
}

func (v *selectExpSQLBuilderImpl) Build() string {
	return fmt.Sprintf("SELECT %v FROM %v", v.fields, v.srcTableName)
}

// FilterExpSQLBuilder

type (
	FilterExpSQLBuilder interface {
		SelectExpSQLBuilder
		WithFilter(filter string) FilterExpSQLBuilder
	}
	filterExpSQLBuilderImpl struct {
		*selectExpSQLBuilderImpl
		filter string
	}
)

func NewFilterExpSQLBuilder() FilterExpSQLBuilder {
	return &filterExpSQLBuilderImpl{selectExpSQLBuilderImpl: &selectExpSQLBuilderImpl{}}
}

func (v *filterExpSQLBuilderImpl) Build() string {
	return fmt.Sprintf("%v WHERE %v", v.selectExpSQLBuilderImpl.Build(), v.filter)
}

func (v *filterExpSQLBuilderImpl) WithFilter(filter string) FilterExpSQLBuilder {
	v.filter = filter
	return v
}

type (
	TumblingCntWindowExpSQLBuilder interface {
		FlinkSQLBuilder
		WithQueryTable(name string) TumblingCntWindowExpSQLBuilder
		WithEntities(field []string) TumblingCntWindowExpSQLBuilder
		WithAttributes(field []string) TumblingCntWindowExpSQLBuilder
		WithTimestampField(ts string) TumblingCntWindowExpSQLBuilder
		WithMinuteInterval(interval int64) TumblingCntWindowExpSQLBuilder
	}
	tumblingCntWindowExpSQLBuilderImpl struct {
		srcTableName   string
		entities       []string
		attributes     []string
		tsField        string
		minuteInterval int64
	}
)

func NewTumblingCntWindowExpSQLBuilder() TumblingCntWindowExpSQLBuilder {
	return &tumblingCntWindowExpSQLBuilderImpl{entities: make([]string, 0), attributes: make([]string, 0)}
}

func (v *tumblingCntWindowExpSQLBuilderImpl) Build() string {
	return fmt.Sprintf("SELECT window_start,window_end,COUNT(*) AS `cnt`,%v "+
		"FROM TABLE(TUMBLE(TABLE %v, DESCRIPTOR(%v),INTERVAL '%v' MINUTES)) "+
		"GROUP BY window_start,window_end, %v", v.buildQueryField(), v.srcTableName, v.tsField, v.minuteInterval, v.buildGroupByField())
}

func (v *tumblingCntWindowExpSQLBuilderImpl) WithQueryTable(name string) TumblingCntWindowExpSQLBuilder {
	v.srcTableName = name
	return v
}

func (v *tumblingCntWindowExpSQLBuilderImpl) WithTimestampField(ts string) TumblingCntWindowExpSQLBuilder {
	v.tsField = ts
	return v
}

func (v *tumblingCntWindowExpSQLBuilderImpl) WithEntities(fields []string) TumblingCntWindowExpSQLBuilder {
	v.entities = fields
	return v
}

func (v *tumblingCntWindowExpSQLBuilderImpl) WithAttributes(fields []string) TumblingCntWindowExpSQLBuilder {
	v.attributes = fields
	return v
}

func (v *tumblingCntWindowExpSQLBuilderImpl) WithMinuteInterval(interval int64) TumblingCntWindowExpSQLBuilder {
	v.minuteInterval = interval
	return v
}

func (v *tumblingCntWindowExpSQLBuilderImpl) buildQueryField() string {
	fields := ""
	if len(v.entities) == 1 {
		fields = fields + v.entities[0] + " AS entities,"
	} else {
		fields = fields + fmt.Sprintf("concat_ws('_',%v,%v)", v.entities[0], v.entities[1]) + " AS entities,"
	}
	if len(v.attributes) == 1 {
		fields = fields + v.attributes[0] + " AS attributes"
	} else {
		fields = fields + fmt.Sprintf("concat_ws('_',%v,%v)", v.attributes[0], v.attributes[1]) + " AS attributes"
	}
	return fields
}

func (v *tumblingCntWindowExpSQLBuilderImpl) buildGroupByField() string {
	fields := ""
	for _, f := range v.entities {
		fields = fields + f + ","
	}
	for _, f := range v.attributes {
		fields = fields + f + ","
	}
	fields = fields[:len(fields)-1]
	return fields
}

type (
	InsertSQLBuilder interface {
		FlinkSQLBuilder
		WithDestinationTable(name string) InsertSQLBuilder
		WithExpression(exp string) InsertSQLBuilder
	}
	insertSQLBuilderImpl struct {
		destinationTableName string
		expression           string
	}
)

func NewInsertSQLBuilder() InsertSQLBuilder {
	return &insertSQLBuilderImpl{}
}

func (v *insertSQLBuilderImpl) WithDestinationTable(name string) InsertSQLBuilder {
	v.destinationTableName = name
	return v
}

func (v *insertSQLBuilderImpl) WithExpression(exp string) InsertSQLBuilder {
	v.expression = exp
	return v
}

func (v *insertSQLBuilderImpl) Build() string {
	return fmt.Sprintf("INSERT INTO %v %v", v.destinationTableName, v.expression)
}

type (
	StatementSetSQLBuilder interface {
		FlinkSQLBuilder
		WithInsertStatement(stm string) StatementSetSQLBuilder
	}
	statementSetSQLBuilderImpl struct {
		statements []string
	}
)

func NewStatementSetSQLBuilder() StatementSetSQLBuilder {
	return &statementSetSQLBuilderImpl{}
}

func (v *statementSetSQLBuilderImpl) WithInsertStatement(stm string) StatementSetSQLBuilder {
	v.statements = append(v.statements, stm)
	return v
}

func (v *statementSetSQLBuilderImpl) Build() string {
	executeStr := "EXECUTE STATEMENT SET "
	beginStr := "BEGIN"
	endStr := "END;"
	stms := ""
	for _, stm := range v.statements {
		normalizeStm := fmt.Sprintf(" %v;", stm)
		stms = stms + normalizeStm
	}
	return executeStr + beginStr + stms + endStr
}

type (
	SetConfigSQLBuilder interface {
		FlinkSQLBuilder
		WithConfig(key string, value string) SetConfigSQLBuilder
	}
	setConfigSQLBuilderImpl struct {
		key   string
		value string
	}
)

func NewSetConfigSQLBuilder() SetConfigSQLBuilder {
	return &setConfigSQLBuilderImpl{}
}

func (v *setConfigSQLBuilderImpl) WithConfig(key string, value string) SetConfigSQLBuilder {
	v.key = key
	v.value = value
	return v
}

func (v *setConfigSQLBuilderImpl) Build() string {
	return fmt.Sprintf("SET '%v' = '%v'", v.key, v.value)
}
