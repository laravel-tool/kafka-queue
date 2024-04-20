<?php

namespace LaravelTool\KafkaQueue\Kafka;

use RdKafka\Conf as KafkaConfig;
use RdKafka\Exception as KafkaException;
use RdKafka\KafkaConsumer;
use RdKafka\Message;
use RuntimeException;

class Consumer
{
    private KafkaConsumer $consumer;

    public function __construct(
        protected array $config
    ) {
        $this->consumer = new KafkaConsumer($this->generateConfig($config));
    }

    public function consume(string $topic): ?Message
    {
        try {
            $this->checkSubscription($topic);

            $message = $this->consumer->consume($this->config['consumer_timeout_ms']);
        } catch (KafkaException) {
            return null;
        }

        return match ($message->err) {
            RD_KAFKA_RESP_ERR_NO_ERROR => $message,
            RD_KAFKA_RESP_ERR__PARTITION_EOF, RD_KAFKA_RESP_ERR__TIMED_OUT, RD_KAFKA_RESP_ERR__UNKNOWN_PARTITION, RD_KAFKA_RESP_ERR__UNKNOWN_TOPIC, RD_KAFKA_RESP_ERR_UNKNOWN_TOPIC_OR_PART => null,
            default => throw new RuntimeException($message->errstr(), $message->err),
        };
    }

    /**
     * @throws KafkaException
     */
    public function commit():void
    {
        $this->consumer->commit();
    }

    private function generateConfig(array $config): KafkaConfig
    {
        $kafkaConfig = new KafkaConfig();
        $kafkaConfig->set('metadata.broker.list', $config['broker_list']);
        $kafkaConfig->set('group.id', $config['group_name']);
        $kafkaConfig->set('heartbeat.interval.ms', $config['heartbeat_ms']);
        $kafkaConfig->set('auto.offset.reset', 'earliest');

        return $kafkaConfig;
    }

    /**
     * @throws KafkaException
     */
    private function checkSubscription(string $topic): void
    {
        if (!in_array($topic, $this->consumer->getSubscription())) {
            $this->consumer->subscribe([$topic]);
        }
    }
}
