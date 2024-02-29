package config

import (
	"fmt"
	"log"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

// Retrieve config from environmental variables

// Configuration will be pulled from the environment using the following keys
const (
	envServerURL = "MQTT_BrokerURL" // server URL

	envAuth         = "MQTT_IsAuth"
	envAuthUser     = "MQTT_User"
	envAuthPassword = "MQTT_Password"

	envTopic  = "MQTT_Topic" // topic to publish on
	envClient = "MQTT_ClientID"
	envGroup  = "MQTT_Group" // topic to publish on
	envQos    = "MQTT_Qos"   // qos to utilise when publishing

	envKeepAlive         = "MQTT_KeepAlive"         // seconds between keepalive packets
	envConnectRetryDelay = "MQTT_ConnectRetryDelay" // milliseconds to delay between connection attempts

	envWriteToStdOut = "MQTT_WriteToStdout" // if "true" then received packets will be written stdout
	envWriteToDisk   = "MQTT_WriteToDisk"   // if "true" then received packets will be written to file
	envOutputFile    = "MQTT_OutputFile"    // name of file to use if above is true

	envQueuePath = "MQTT_QueuePath"

	envDebug = "MQTT_Debug" // if "true" then the libraries will be instructed to print debug info
)

type authConf struct {
	UserName string
	Password []byte
}

// config holds the configuration
type Config struct {
	ServerURL *url.URL // MQTT server URL
	Auth      *authConf
	ClientID  string // Client ID to use when connecting to server
	Topic     string // Topic on which to publish messaged
	Qos       byte   // QOS to use when publishing

	KeepAlive         uint16        // seconds between keepalive packets
	ConnectRetryDelay time.Duration // Period between connection attempts

	WriteToStdOut  bool   // If true received messages will be written to stdout
	WriteToDisk    bool   // if true received messages will be written to below file
	OutputFileName string // filename to save messages to
	QueuePath      string

	Debug bool // autopaho and paho debug output requested

	Logger *log.Logger
}

func (c *Config) SetAuth(user string, pass []byte) {
	c.Auth = &authConf{UserName: user, Password: pass}
}

func (c *Config) IsRawdata() bool {
	return strings.Contains(c.Topic, "v1/rawdata")
}

// getConfig - Retrieves the configuration from the environment
func GetConfigFronEnv() (*Config, error) {
	var cfg Config
	var err error

	var srvURL string
	if srvURL, err = stringFromEnv(envServerURL); err != nil {
		return nil, err
	}

	cfg.ServerURL, err = url.Parse(srvURL)
	if err != nil {
		return nil, fmt.Errorf("environmental variable %s must be a valid URL (%w)", envServerURL, err)
	}
	name, err := os.Hostname()
	if err != nil {
		return nil, err
	}

	if clientID, err := stringFromEnv(envClient); err != nil {
		return nil, err
	} else {
		cfg.ClientID = fmt.Sprintf("%s-%s", clientID, name)
	}

	if cfg.Topic, err = stringFromEnv(envTopic); err != nil {
		return nil, err
	}

	group, err := stringFromEnv(envGroup)
	if err != nil {
		return nil, err
	}
	if group != "" {
		cfg.Topic = fmt.Sprintf("$share/%s/%s", group, cfg.Topic)
	}

	cfg.QueuePath, err = stringFromEnv(envQueuePath)
	if err != nil {
		return nil, err
	}

	iQos, err := intFromEnv(envQos)
	if err != nil {
		return nil, err
	}
	cfg.Qos = byte(iQos)

	iKa, err := intFromEnv(envKeepAlive)
	if err != nil {
		return nil, err
	}
	cfg.KeepAlive = uint16(iKa)

	if cfg.ConnectRetryDelay, err = milliSecondsFromEnv(envConnectRetryDelay); err != nil {
		return nil, err
	}

	if cfg.WriteToStdOut, err = booleanFromEnv(envWriteToStdOut); err != nil {
		return nil, err
	}
	if cfg.WriteToDisk, err = booleanFromEnv(envWriteToDisk); err != nil {
		return nil, err
	}
	if cfg.OutputFileName, err = stringFromEnv(envOutputFile); cfg.WriteToDisk && err != nil {
		return nil, err
	}

	if cfg.Debug, err = booleanFromEnv(envDebug); err != nil {
		return nil, err
	}

	isAuth, err := booleanFromEnv(envAuth)
	if err != nil {
		return nil, err
	}
	if isAuth {
		cfg.Auth = &authConf{}
		if cfg.Auth.UserName, err = stringFromEnv(envAuthUser); err != nil {
			return nil, err
		}
		password, err := stringFromEnv(envAuthPassword)
		if err != nil {
			return nil, err
		}
		cfg.Auth.Password = []byte(password)
	}

	return &cfg, nil
}

// stringFromEnv - Retrieves a string from the environment and ensures it is not blank (ort non-existent)
func stringFromEnv(key string) (string, error) {
	s := os.Getenv(key)
	if len(s) == 0 {
		return "", fmt.Errorf("environmental variable %s must not be blank", key)
	}
	return s, nil
}

// intFromEnv - Retrieves an integer from the environment (must be present and valid)
func intFromEnv(key string) (int, error) {
	s := os.Getenv(key)
	if len(s) == 0 {
		return 0, fmt.Errorf("environmental variable %s must not be blank", key)
	}
	i, err := strconv.Atoi(s)
	if err != nil {
		return 0, fmt.Errorf("environmental variable %s must be an integer", key)
	}
	return i, nil
}

// milliSecondsFromEnv - Retrieves milliseconds (as time.Duration) from the environment (must be present and valid)
func milliSecondsFromEnv(key string) (time.Duration, error) {
	s := os.Getenv(key)
	if len(s) == 0 {
		return 0, fmt.Errorf("environmental variable %s must not be blank", key)
	}
	i, err := strconv.Atoi(s)
	if err != nil {
		return 0, fmt.Errorf("environmental variable %s must be an integer", key)
	}
	return time.Duration(i) * time.Millisecond, nil
}

// booleanFromEnv - Retrieves boolean from the environment (must be present and valid)
func booleanFromEnv(key string) (bool, error) {
	s := os.Getenv(key)
	if len(s) == 0 {
		return false, fmt.Errorf("environmental variable %s must not be blank", key)
	}
	switch strings.ToUpper(s) {
	case "TRUE", "T", "1":
		return true, nil
	case "FALSE", "F", "0":
		return false, nil
	default:
		return false, fmt.Errorf("environmental variable %s be a valid boolean option (is %s)", key, s)
	}
}
