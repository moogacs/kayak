package service

type EventType int

const (
	EventServerListening EventType = iota
	EventServerStopped
)

type Event interface {
	EventType() EventType
}

type ServerEvent struct {
	Type EventType
}

func (m ServerEvent) EventType() EventType {
	return m.Type
}
