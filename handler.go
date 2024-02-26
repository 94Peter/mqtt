package mqtt

import (
	"bytes"
	"fmt"
	"os"
	"sync"
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
	}
}

// Close closes the file
func (o *handler) Close() {
	if o.f != nil {
		if err := o.f.Close(); err != nil {
			fmt.Printf("ERROR closing file: %s", err)
		}
		o.f = nil
	}
}

var handlerWaitGroup sync.WaitGroup

// handle is called when a message is received
func (o *handler) handle(msg *paho.PublishReceived) {
	// We extract the count and write that out first to simplify checking for missing values
	if bytes.Contains(msg.Packet.Payload, []byte{'@'}) ||
		bytes.Contains(msg.Packet.Payload, []byte{'\x01'}) {
		return
	}

	if o.f != nil {
		// Write out the number (make it long enough that sorting works) and the payload
		if _, err := o.f.WriteString(fmt.Sprintf("%s\n", msg.Packet.Payload)); err != nil {
			fmt.Printf("ERROR writing to file: %s", err)
		}
	}

	var err error
	for _, t := range o.trans {
		if t.IsValid() {
			err = t.Send(msg.Packet.Payload)
			if err != nil {
				fmt.Println("send fail: " + err.Error())
				fmt.Println(string(msg.Packet.Payload))
				syscall.SIGTERM.Signal()
			}
		}
	}
}
