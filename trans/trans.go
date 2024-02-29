package trans

type Trans interface {
	Send([]byte) error
	Close()
}
