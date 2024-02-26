package trans

type Trans interface {
	Connect() error
	Send([]byte) error
	Close()
	IsValid() bool
}
