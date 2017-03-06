package gokafka

import (
	"io/ioutil"
	"log"
	"sync/atomic"
	"time"

	"github.com/Shopify/sarama"
	"github.com/Sirupsen/logrus"
)

// New takes a set of options and returns a new kafka instance or an error
func New(opts ...Option) (*Kafka, error) {
	k := Kafka{
		writeSig: make(chan struct{}),
		stream:   make(chan *sarama.ConsumerMessage),
		quitCh:   make(chan struct{}),
	}

	for _, opt := range opts {
		if err := opt.Apply(&k); err != nil {
			return nil, err
		}
	}

	if k.logger == nil {
		k.logger = logrus.New()
		k.logger.Out = ioutil.Discard
	}

	go k.timer()

	return &k, nil
}

// Kafka is a wrapper around a sarama.Consumer
type Kafka struct {
	options        runOpts
	consumer       sarama.Consumer
	writeSig       chan struct{}
	funcs          map[string]func(*sarama.ConsumerMessage) error
	offsetWriter   OffsetHandler
	stream         chan *sarama.ConsumerMessage
	totalConsumers int64
	quitCh         chan struct{}
	logger         *logrus.Logger
}

type runOpts struct {
	defaultOffset int64
}

// Start starts the connections to a kafka instance
func (k *Kafka) Start() error {
	k.logger.Info("requesting topics")
	topics, err := k.consumer.Topics()
	if err != nil {
		k.logger.WithError(err).Error("error getting topics")
		return err
	}
	k.logger.WithField("topics", topics).Info("got topics")

	// loop over all topics
	for _, topic := range topics {

		// check if there's a registered func for the topic
		topicEntry := k.logger.WithField("topic", topic)
		topicEntry.Info("handling Topic")
		if fn, ok := k.funcs[topic]; ok {
			topicEntry.Info("topic is registered")

			// get partitions
			topicEntry.Info("fetching partitions")
			partitions, err := k.consumer.Partitions(topic)
			if err != nil {
				topicEntry.WithError(err).Error("error getting partitions")
				return err
			}

			// start listener for each partition

			for _, partition := range partitions {
				partitionEntry := topicEntry.WithField("partition", partition)

				partitionEntry.Info("fetching offset")
				// get the last offset or the newset record
				offset, err := k.offsetWriter.ReadOffset(topic, partition)
				if err != nil {
					offset = k.options.defaultOffset
				}

				partitionEntry.WithField("offset", offset).Info("request consumption")
				// setup input stream from kafka
				input, err := k.consumer.ConsumePartition(topic, partition, offset)
				if err != nil {
					partitionEntry.WithError(err).Error("unable to consume")
					return err
				}

				partitionEntry.WithField("offset", offset).Info("starting consumer")

				k.totalConsumers++
				go func(l *logrus.Entry, input sarama.PartitionConsumer, fn func(*sarama.ConsumerMessage) error) {
					var rm *sarama.ConsumerMessage

					for {
						select {
						case <-k.writeSig:
							if rm != nil {
								l.WithField("offset", rm.Offset).Info("attempting offset write")
								if err := k.offsetWriter.WriteOffset(rm.Topic, rm.Partition, rm.Offset); err != nil {
									log.Printf("Error writing to offest writer: %s", err.Error())
								}

								rm = nil
							} else {
								l.Info("no new offset")
							}
						case message := <-input.Messages():
							l.WithField("offset", message.Offset).Info("got message")
							rm = message

							if err := fn(message); err != nil {
								l.WithError(err).WithField("message", string(message.Value)).Error("unable to handle event")
							}
						case err := <-input.Errors():
							// TODO: this should be handled better -- but this is just a demo, so we just log the error.
							l.WithError(err).Error("got error from kafka")
						case <-k.quitCh:
							l.Info("quitting")
							atomic.AddInt64(&k.totalConsumers, -1)

							if err := input.Close(); err != nil {
								// TODO: handle this later
								l.WithError(err).Error("error closing kafka connection")
							}
						}
					}
				}(partitionEntry, input, fn)
			}
		}
	}

	return nil
}

// Stop gracefully stops the kafka subscriptions
func (k *Kafka) Stop() error {
	k.logger.Info("stopping kafka")
	k.quitCh <- struct{}{}

	return nil
}

// timer is responsible for triggering a signal to persist
// the offset of each worker to the offsetWriter.
func (k *Kafka) timer() {

	ticker := time.NewTicker(time.Second * 5)

	for {
		select {
		case <-ticker.C:
			k.logger.Info("broadcasting write offsets")
			for i := 0; i < int(k.totalConsumers); i++ {
				k.writeSig <- struct{}{}
			}
		case <-k.quitCh:
			ticker.Stop()
			return
		}
	}
}
