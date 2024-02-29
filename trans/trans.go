package trans

type Trans interface {
	Send([]byte) error
	Close()
}

func NewSimpleTrans(myFunc func([]byte) error) Trans {
	return &simpleTrans{sendFunc: myFunc}
}

type simpleTrans struct {
	sendFunc func([]byte) error
}

func (t *simpleTrans) Send(data []byte) error {
	return t.sendFunc(data)
}

func (t *simpleTrans) Close() {
}
