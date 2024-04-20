<?php

namespace LaravelTool\KafkaQueue;

use Illuminate\Container\Container;
use Illuminate\Queue\Jobs\Job as BaseJob;
use Illuminate\Contracts\Queue\Job as JobInterface;
use RdKafka\Message;

class Job extends BaseJob implements JobInterface
{
    protected array $decoded;

    public function __construct(
        Container $container,
        protected Queue $kafkaQueue,
        protected string $job,
        protected Message $message,
    ) {
        $this->container = $container;
        $this->decoded = $this->payload();
        $this->connectionName = $this->kafkaQueue->getConnectionName();
        $this->queue = $this->message->topic_name;
    }

    public function getJobId()
    {
        return $this->decoded['id'] ?? null;
    }

    public function getRawBody(): string
    {
        return $this->job;
    }

    public function attempts()
    {
        return ($this->decoded['attempts'] ?? null) + 1;
    }

    public function release($delay = 0): void
    {
        parent::release($delay);

        $this->delete();
        $this->kafkaQueue->release($delay, $this);
    }

    public function delete(): void
    {
        parent::delete();

        $this->kafkaQueue->deleteReserved($this->queue, $this);
    }

    public function getMessageTimestamp(): int
    {
        return (int) ($this->message->timestamp / 1000);
    }
}
