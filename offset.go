package gokafka

// OffsetHandler takes a topic, partition and offset and saves the offest
type OffsetHandler interface {
	WriteOffset(topic string, partition int32, offset int64) error
	ReadOffset(topic string, partition int32) (int64, error)
}
