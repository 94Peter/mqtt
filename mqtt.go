package mqtt

import (
	"context"
	"crypto/tls"
	"fmt"
	"net/url"
	"time"

	"github.com/94peter/mqtt/config"
	"github.com/eclipse/paho.golang/autopaho"
	"github.com/eclipse/paho.golang/paho"
)

type MqttServer interface {
	Publish(topic string, qos byte, payload []byte) error
	MqttSubOnlyServer
}

type MqttSubOnlyServer interface {
	Run(ctx context.Context)
	Close()
}

func NewMqttSubOnlyServ(conf *config.Config, h Handler) MqttSubOnlyServer {
	return &mqttServ{
		config: conf,
		h:      h,
	}
}

func NewMqttServ(conf *config.Config, h Handler) MqttServer {
	return &mqttServ{
		config: conf,
		h:      h,
	}
}

type mqttServ struct {
	ctx    context.Context
	config *config.Config
	cm     *autopaho.ConnectionManager
	h      Handler
}

func (serv *mqttServ) Close() {
	if serv.h != nil {
		serv.h.Close()
	}
}

func (serv *mqttServ) Run(ctx context.Context) {
	serv.ctx = ctx
	cfg := serv.config
	cliCfg := autopaho.ClientConfig{
		BrokerUrls:        []*url.URL{cfg.ServerURL},
		KeepAlive:         cfg.KeepAlive,
		ConnectRetryDelay: cfg.ConnectRetryDelay,
		// SessionExpiryInterval - Seconds that a session will survive after disconnection.
		// It is important to set this because otherwise, any queued messages will be lost if the connection drops and
		// the server will not queue messages while it is down. The specific setting will depend upon your needs
		// (60 = 1 minute, 3600 = 1 hour, 86400 = one day, 0xFFFFFFFE = 136 years, 0xFFFFFFFF = don't expire)
		SessionExpiryInterval: 86400,
		OnConnectionUp: func(cm *autopaho.ConnectionManager, connAck *paho.Connack) {
			fmt.Println("mqtt connection up")
			// Subscribing in the OnConnectionUp callback is recommended (ensures the subscription is reestablished if
			// the connection drops)
			if _, err := cm.Subscribe(context.Background(), &paho.Subscribe{
				Subscriptions: []paho.SubscribeOptions{
					{Topic: cfg.Topic, QoS: 1},
				},
			}); err != nil {
				fmt.Printf("failed to subscribe (%s). This is likely to mean no messages will be received.", err)
			}
			fmt.Println("mqtt subscription made")
		},
		OnConnectError: func(err error) { fmt.Printf("error whilst attempting connection: %s\n", err) },
		ClientConfig: paho.ClientConfig{
			// If you are using QOS 1/2, then it's important to specify a client id (which must be unique)
			ClientID: cfg.ClientID,
			// OnPublishReceived is a slice of functions that will be called when a message is received.
			// You can write the function(s) yourself or use the supplied Router
			OnPublishReceived: []func(paho.PublishReceived) (bool, error){
				func(pr paho.PublishReceived) (bool, error) {
					serv.h.handle(&pr)
					fmt.Printf("received message on topic %s; body: %s (retain: %t)\n", pr.Packet.Topic, pr.Packet.Payload, pr.Packet.Retain)
					return true, nil
				}},
			OnClientError: func(err error) { fmt.Printf("client error: %s\n", err) },
			OnServerDisconnect: func(d *paho.Disconnect) {
				if d.Properties != nil {
					fmt.Printf("server requested disconnect: %s\n", d.Properties.ReasonString)
				} else {
					fmt.Printf("server requested disconnect; reason code: %d\n", d.ReasonCode)
				}
			},
		},
	}

	if cfg.Debug {
		cliCfg.Debug = logger{prefix: "autoPaho"}
		cliCfg.PahoDebug = logger{prefix: "paho"}
	}

	if cfg.Auth != nil {
		cliCfg.ConnectPassword = cfg.Auth.Password
		cliCfg.ConnectUsername = cfg.Auth.UserName
	}

	if cfg.ServerURL.Scheme == "ssl" {
		cliCfg.TlsCfg = &tls.Config{
			ClientAuth:         tls.NoClientCert,
			ClientCAs:          nil,
			InsecureSkipVerify: true,
		}
	}

	//
	// Connect to the broker
	//
	var err error
	serv.cm, err = autopaho.NewConnection(ctx, cliCfg)
	if err != nil {
		panic(err)
	}

	// Wait for the connection to come up
	if err = serv.cm.AwaitConnection(ctx); err != nil {
		panic(err)
	}

	// Messages will be handled through the callback so we really just need to wait until a shutdown
	// is requested
	<-ctx.Done()
	fmt.Println("signal caught - exiting subscribe")

	// We could cancel the context at this point but will call Disconnect instead (this waits for autopaho to shutdown)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_ = serv.cm.Disconnect(ctx)
	fmt.Println("shutdown subscribe complete")
}

func (serv *mqttServ) Publish(topic string, qos byte, payload []byte) error {
	// Publish will block so we run it in a goRoutine
	pr, err := serv.cm.Publish(serv.ctx, &paho.Publish{
		QoS:     qos,
		Topic:   topic,
		Payload: payload,
	})
	if err != nil {
		fmt.Printf("error publishing: %s\n", err)
	} else if pr.ReasonCode != 0 && pr.ReasonCode != 16 { // 16 = Server received message but there are no subscribers
		fmt.Printf("reason code %d received\n", pr.ReasonCode)
	} else {
		fmt.Printf("sent topic [%s] message: %s\n", topic, payload)
	}
	return nil
}

type logger struct {
	prefix string
}

// Println is the library provided NOOPLogger's
// implementation of the required interface function()
func (l logger) Println(v ...interface{}) {
	fmt.Println(append([]interface{}{l.prefix + ":"}, v...)...)
}

// Printf is the library provided NOOPLogger's
// implementation of the required interface function(){}
func (l logger) Printf(format string, v ...interface{}) {
	if len(format) > 0 && format[len(format)-1] != '\n' {
		format = format + "\n" // some log calls in paho do not add \n
	}
	fmt.Printf(l.prefix+":"+format, v...)
}
