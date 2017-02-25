package eventbus

type Message struct {
	ReplyTo string
	Payload []byte
}
