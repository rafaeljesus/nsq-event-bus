package bus

type Message struct {
	ReplyTo string
	Payload []byte
}
