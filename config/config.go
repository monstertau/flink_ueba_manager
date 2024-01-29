package config

import (
	"github.com/pkg/errors"
	"github.com/spf13/viper"
)

const (
	DefaultConfigFilePath = "./config.yml"
	// DefServiceHost default endpoint address
	DefServiceHost     = "0.0.0.0"
	DefServicePort     = 9999
	DefEndpointGetJobs = "http://localhost:9090/api/v2/worker/stateless/jobs"
)

var AppConfig *Config

func init() {
	AppConfig = DefaultConfig()
}

type (
	Config struct {
		Service         *NodeConfig      `mapstructure:"service" json:"service"`
		Endpoint        *Endpoint        `mapstructure:"endpoint" json:"endpoint"`
		FlinkSQLGateway *FlinkSQLGateway `mapstructure:"flink_sql_gateway" json:"flink_sql_gateway"`
		KafkaGroupID    string           `mapstructure:"kafka_group_id" json:"kafka_group_id"`
	}

	FlinkSQLGateway struct {
		URL string `mapstructure:"url" json:"url"`
	}

	NodeConfig struct {
		Host string `mapstructure:"host" json:"host"`
		Port int    `mapstructure:"port" json:"port"`
	}
	Endpoint struct {
		BehaviorGetJob string `mapstructure:"behavior_get_job" json:"behavior_get_job"`
		RuleGetJob     string `mapstructure:"rule_get_job" json:"rule_get_job"`
	}
)

func LoadFile(filepath string) (*Config, error) {
	v := viper.New()
	v.SetConfigFile(filepath)
	v.SetConfigType("yaml")

	if err := v.ReadInConfig(); err != nil {
		if _, ok := err.(viper.ConfigFileNotFoundError); ok {
			return nil, errors.New("configuration file not found")
		} else {
			return nil, errors.Wrap(err, "ReadInConfig")
		}
	}
	config := DefaultConfig()
	if err := v.Unmarshal(&config); err != nil {
		return nil, errors.Wrap(err, "viper.Unmarshal")
	}
	AppConfig = config
	return config, nil
}

func DefaultEndpoint() *Endpoint {
	return &Endpoint{
		BehaviorGetJob: DefEndpointGetJobs,
		RuleGetJob:     DefEndpointGetJobs,
	}
}

func DefaultNodeConfig() *NodeConfig {
	return &NodeConfig{
		Host: DefServiceHost,
		Port: DefServicePort,
	}
}

func DefaultFlinkSQLGatewayConfig() *FlinkSQLGateway {
	return &FlinkSQLGateway{
		URL: "http://localhost:8083",
	}
}

func DefaultConfig() *Config {
	return &Config{
		Service:  DefaultNodeConfig(),
		Endpoint: DefaultEndpoint(),
	}
}

func GetConfig() *Config {
	if AppConfig == nil {
		panic("init config first")
	}
	return AppConfig
}
