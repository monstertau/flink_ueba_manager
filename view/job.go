package view

type (
	ProfileConfig struct {
		ID             string    `json:"id"`
		Name           string    `json:"name"`
		Status         int64     `json:"status"`
		ProfileType    string    `json:"profile_type"`
		Entities       []*Object `json:"entity"`
		Attributes     []*Object `json:"attribute"`
		ProfileTime    string    `json:"profile_time"`
		SavingDuration int64     `json:"saving_duration_minute"`
		Threshold      float64   `json:"threshold"`
	}
	Object struct {
		Name      string                 `json:"field_name" example:"server_id"`
		Type      string                 `json:"type" enums:"original,mapping,reference" example:"original"`
		ExtraData map[string]interface{} `json:"extra_data"`
	}
	kafkaConfig struct {
		BootstrapServer string            `json:"bootstrap.servers" binding:"required"`
		Topic           string            `json:"topic" binding:"required"`
		AuthenType      string            `json:"authen_type"`
		Keytab          string            `json:"keytab"`
		Principal       string            `json:"principal"`
		Schema          map[string]string `json:"schema"`
		TimestampField  string            `json:"timestamp_field"`
	}
	LogSourceConfig struct {
		Config kafkaConfig `json:"config" binding:"required"`
	}
	BehaviorJobConfig struct {
		ID              string         `json:"id"`
		LogSourceConfig *kafkaConfig   `json:"source_config" binding:"required"`
		ProfileConfig   *ProfileConfig `json:"profile_config" binding:"required"`
		ProfileOutput   *kafkaConfig   `json:"profile_output_config" binding:"required"`
		BehaviorOutput  *kafkaConfig   `json:"behavior_output_config" binding:"required"`
		BehaviorFilter  string         `json:"filter" binding:"required"`
	}
	RuleJobConfig struct {
		ID                     string       `json:"id"`
		Name                   string       `json:"name"`
		Filter                 string       `json:"filter"`
		Object                 string       `json:"object"`
		Technique              string       `json:"technique"`
		Severity               string       `json:"severity"`
		RiskScore              int          `json:"risk_score"`
		ProfilePredictorOutput *kafkaConfig `json:"profile_predictor_config" binding:"required"`
		RuleOutput             *kafkaConfig `json:"rule_output_config" binding:"required"`
	}
)
