<?php

namespace LaravelTool\KafkaQueue\Kafka;

use Closure;
use Illuminate\Support\Str;
use RdKafka\Conf as KafkaConfig;
use RdKafka\Producer as KafkaProducer;

class Producer
{
    use AuthConfigTrait;

    private KafkaProducer $producer;

    public function __construct(
        protected array $config
    ) {
        $this->producer = new KafkaProducer($this->generateConfig($config));
    }

    public function produce(
        string $topic,
        string $payload,
        ?int $delay = null,
        ?callable $callback = null,
    ): void {
        $this->producer->newTopic($topic)->producev(
            partition: RD_KAFKA_PARTITION_UA,
            msgflags: 0,
            payload: $payload,
            key: $this->getMessageKey(),
            timestamp_ms: ($delay ?? time()) * 1000,
        );

        if (is_callable($callback)) {
            $callback();
        }

        $this->producer->poll(0);
        $this->producer->flush($this->config['producer_timeout_ms']);
    }

    private function getMessageKey(): string
    {
        return Str::orderedUuid()->toString();
    }

    private function generateConfig(array $config): KafkaConfig
    {
        $kafkaConfig = new KafkaConfig();
        $kafkaConfig->set('metadata.broker.list', $config['broker_list']);

        $this->authConfig($kafkaConfig, $config);

        return $kafkaConfig;
    }
}
