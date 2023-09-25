package coinmiddleware

import (
	"context"
	"github.com/0xPolygonHermez/zkevm-bridge-service/redisstorage"
	"github.com/0xPolygonHermez/zkevm-node/log"
	"github.com/IBM/sarama"
	"github.com/pkg/errors"
)

// KafkaConsumer provides the interface to consume from coin middleware kafka
type KafkaConsumer interface {
	Start(ctx context.Context) error
	Close() error
}

type kafkaConsumerImpl struct {
	topics  []string
	client  sarama.ConsumerGroup
	handler sarama.ConsumerGroupHandler
}

func NewKafkaConsumer(cfg Config, redisStorage redisstorage.RedisStorage) (KafkaConsumer, error) {
	config := sarama.NewConfig()
	config.Consumer.Offsets.Initial = cfg.InitialOffset

	client, err := sarama.NewConsumerGroup(cfg.Brokers, cfg.ConsumerGroupID, config)
	if err != nil {
		return nil, errors.Wrap(err, "kafka consumer group init error")
	}

	return &kafkaConsumerImpl{
		topics:  cfg.Topics,
		client:  client,
		handler: NewMessageHandler(redisStorage),
	}, nil
}

func (c *kafkaConsumerImpl) Start(ctx context.Context) error {
	log.Debug("starting kafka consumer")
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	for {
		log.Debugf("start consume")
		err := c.client.Consume(ctx, c.topics, c.handler)
		if err != nil {
			log.Errorf("kafka consumer error: %v", err)
			if errors.Is(err, sarama.ErrClosedConsumerGroup) {
				err = nil
			}
			return errors.Wrap(err, "kafka consumer error")
		}
		if err = ctx.Err(); err != nil {
			log.Errorf("kafka consumer ctx error: %v", err)
			return errors.Wrap(err, "kafka consumer ctx error")
		}
	}
}

func (c *kafkaConsumerImpl) Close() error {
	log.Debug("closing kafka consumer...")
	return c.client.Close()
}
