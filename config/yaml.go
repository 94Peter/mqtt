package config

import (
	"fmt"
	"net/url"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

type yamlConfig struct {
	ServerURL         *url.URL      `yaml:"server_url"`
	Auth              *authConf     `yaml:"auth"`
	Qos               byte          `yaml:"qos"`
	KeepAlive         uint16        `yaml:"keepalive"`
	ConnectRetryDelay time.Duration `yaml:"connect_retry_delay"`
	QueuePath         string        `yaml:"queue_path"`
	Debug             bool          `yaml:"debug"`
}

func GetConfigFromYaml(file string, service string) (*Config, error) {
	var yamlCfg yamlConfig
	var err error
	data, err := os.ReadFile(file)
	if err != nil {
		return nil, err
	}

	err = yaml.Unmarshal(data, &yamlCfg)
	if err != nil {
		return nil, err
	}
	hostname, err := os.Hostname()
	if err != nil {
		return nil, err
	}
	var cfg Config

	cfg.ServerURL = yamlCfg.ServerURL
	cfg.ClientID = fmt.Sprintf("%s-%s", service, hostname)
	cfg.Qos = yamlCfg.Qos
	cfg.KeepAlive = yamlCfg.KeepAlive
	cfg.ConnectRetryDelay = yamlCfg.ConnectRetryDelay
	cfg.QueuePath = yamlCfg.QueuePath
	cfg.Debug = yamlCfg.Debug
	cfg.Auth = yamlCfg.Auth

	return &cfg, nil
}
