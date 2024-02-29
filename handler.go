package mqtt

import (
	"log"
	"os"
	"syscall"

	"github.com/94peter/mqtt/config"
	"github.com/94peter/mqtt/trans"

	"github.com/eclipse/paho.golang/paho"
)

// handler is a simple struct that provides a function to be called when a message is received. The message is parsed
// and the count followed by the raw message is written to the file (this makes it easier to sort the file)

type Handler interface {
	Close()
	handle(msg *paho.PublishReceived)
}

type handler struct {
	writeToStdOut bool
	trans         []trans.Trans
	f             *os.File
	logger        *log.Logger
}

// NewHandler creates a new output handler and opens the output file (if applicable)
func NewHandler(conf *config.Config, ts ...trans.Trans) Handler {
	var f *os.File
	if conf.WriteToDisk {
		var err error
		f, err = os.Create(conf.OutputFileName)
		if err != nil {
			panic(err)
		}
	}
	return &handler{
		writeToStdOut: conf.WriteToStdOut,
		trans:         ts,
		f:             f,
		logger:        conf.Logger,
	}
}

// Close closes the file
func (o *handler) Close() {
	if o.f != nil {
		if err := o.f.Close(); err != nil {
			o.printf("ERROR closing file: %s", err)
		}
		o.f = nil
	}
	for _, t := range o.trans {
		t.Close()
	}
}

// handle is called when a message is received
func (o *handler) handle(msg *paho.PublishReceived) {
	var err error
	for _, t := range o.trans {
		err = t.Send(msg.Packet.Payload)
		if err != nil {
			o.println("send fail: " + err.Error())
			o.println(string(msg.Packet.Payload))
			syscall.SIGTERM.Signal()
		}
	}
}

func (o *handler) printf(format string, v ...interface{}) {
	if o.logger == nil {
		return
	}
	o.logger.Printf(format, v...)
}

func (o *handler) println(a ...any) {
	if o.logger == nil {
		return
	}
	o.logger.Println(a...)
}
