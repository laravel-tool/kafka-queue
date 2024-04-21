<?php

namespace LaravelTool\KafkaQueue\Kafka;

use RdKafka\Conf as KafkaConfig;
use RdKafka\Exception as KafkaException;
use RdKafka\KafkaConsumer;
use RdKafka\Message;
use RdKafka\Metadata;
use RdKafka\TopicPartition;
use RuntimeException;

class Consumer
{
    private KafkaConsumer $consumer;
    private Metadata $metadata;
    private array $topics;

    /**
     * @throws KafkaException
     */
    public function __construct(
        protected array $config
    ) {
        $this->consumer = new KafkaConsumer($this->generateConfig($this->config));

        $this->metadata();
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
    public function commit(): void
    {
        $this->consumer->commit();
    }

    public function size(string $topicName): int
    {

        try {
            if ($topic = $this->getTopic($topicName)) {
                $topicPartitions = [];
                $watermarkOffsets = [];

                foreach ($topic->getPartitions() as $partition) {
                    $this->consumer->queryWatermarkOffsets($topicName, $partition->getId(), $low, $high,
                        $this->config['consumer_timeout_ms']);

                    $watermarkOffsets[$partition->getId()] = [$low, $high];
                    $topicPartitions[] = new TopicPartition($topicName, $partition->getId());
                }

                $offsets = $this->consumer->getCommittedOffsets($topicPartitions, $this->config['consumer_timeout_ms']);
                $size = 0;
                foreach ($offsets as $offset) {
                    /** @var TopicPartition $offset */
                    $size += $watermarkOffsets[$offset->getPartition()][1] - max($offset->getOffset(),
                            $watermarkOffsets[$offset->getPartition()][0]);
                }

                return $size;
            } else {
                return 0;
            }
        } catch (KafkaException) {
            return 0;
        }
    }

    /**
     * @throws KafkaException
     */
    private function getTopic(string $topic): ?Metadata\Topic
    {
        if (!isset($this->topics[$topic])) {
            $this->metadata();
        }
        return $this->topics[$topic] ?? null;
    }

    /**
     * @throws KafkaException
     */
    private function metadata(): void
    {
        $this->metadata = $this->consumer->getMetadata(true, null, $this->config['consumer_timeout_ms']);
        foreach ($this->metadata->getTopics() as $topic) {
            $this->topics[$topic->getTopic()] = $topic;
        }
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
