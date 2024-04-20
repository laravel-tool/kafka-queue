<?php

namespace LaravelTool\KafkaQueue;

use Illuminate\Contracts\Queue\Queue as QueueInterface;
use Illuminate\Queue\Connectors\ConnectorInterface;
use LaravelTool\KafkaQueue\Kafka\Consumer;
use LaravelTool\KafkaQueue\Kafka\Producer;

class Connector implements ConnectorInterface
{

    public function connect(array $config): QueueInterface
    {
        return new Queue(
            new Producer($config),
            new Consumer($config),
            defaultQueue: $config['queue'] ?? 'default',
            horizon: $config['horizon'] ?? false,
        );
    }

}
