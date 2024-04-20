<?php

namespace LaravelTool\KafkaQueue;

use Illuminate\Contracts\Events\Dispatcher;
use Illuminate\Contracts\Queue\Queue as QueueInterface;
use Illuminate\Queue\Queue as BaseQueue;
use Laravel\Horizon\Events\JobDeleted;
use Laravel\Horizon\Events\JobPushed;
use Laravel\Horizon\Events\JobReserved;
use Laravel\Horizon\JobPayload;
use LaravelTool\KafkaQueue\Kafka\Consumer;
use LaravelTool\KafkaQueue\Kafka\Producer;
use RdKafka\Exception as KafkaException;

class Queue extends BaseQueue implements QueueInterface
{
    protected string|object $lastPushed;

    public function __construct(
        protected Producer $producer,
        protected Consumer $consumer,
        protected string $defaultQueue,
        protected bool $horizon,
    ) {
    }

    public function size($queue = null): int
    {
        return 0;
    }

    public function readyNow(string $queue = null): int
    {
        return $this->size($queue);
    }

    public function push($job, $data = '', $queue = null): ?string
    {
        $queue = $this->getQueueName($queue);

        $this->lastPushed = $job;

        return $this->pushRaw(
            $this->createPayload($job, $queue, $data),
            $queue
        );
    }

    public function pushRaw($payload, $queue = null, array $options = []): ?string
    {
        if ($this->horizon) {
            $payload = (new JobPayload($payload))->prepare($this->lastPushed)->value;
        }

        $this->producer->produce(
            topic: $queue,
            payload: $payload,
            delay: $options['available_at'] ?? null,
        );

        if ($this->horizon) {
            $this->event($this->getQueueName($queue), new JobPushed($payload));
        }

        return json_decode($payload, true)['uuid'] ?? null;
    }

    public function later($delay, $job, $data = '', $queue = null): ?string
    {
        $queueName = $this->getQueueName($queue);

        $this->lastPushed = $job;

        return $this->pushRaw(
            payload: $this->createPayload($job, $queueName, $data),
            queue: $queueName,
            options: [
                'available_at' => $this->availableAt($delay),
            ]
        );
    }

    /**
     * @throws KafkaException
     */
    public function pop($queue = null, ?string $exceptUuid = null): ?Job
    {
        $message = $this->consumer->consume($queue);

        if (is_null($message)) {
            return null;
        }

        $job = new Job(
            container: $this->container,
            kafkaQueue: $this,
            job: $message->payload,
            message: $message,
        );

        if ($job->getMessageTimestamp() > time()) {
            $this->requeue($job);

            if ($job->uuid() === $exceptUuid) {
                return null;
            }

            $this->pop($queue, $exceptUuid ?? $job->uuid());

            return null;
        }

        $this->consumer->commit();

        $this->event($this->getQueueName($queue), new JobReserved($job->getRawBody()));

        return $job;
    }

    public function release($delay, $job): ?string
    {
        $body = $job->payload();
        if (isset($body['data']['command']) === true) {
            $job = unserialize($body['data']['command']);
        } else {
            $job = $job->getName();
        }
        $data = $body['data'];

        $queueName = $this->getQueueName($job->getQueue());

        $payload = $this->createPayload(
            job: $job,
            queue: $queueName,
            data: $data
        );

        $payloadDecoded = json_decode($payload, true);
        $payloadDecoded['attempts'] = $job->attempts();
        $payload = json_encode($payloadDecoded, JSON_UNESCAPED_UNICODE);

        $this->lastPushed = $job;

        return $this->pushRaw(
            payload: $payload,
            queue: $queueName,
            options: $delay ? ['available_at' => $this->availableAt($delay)] : []
        );
    }

    public function deleteReserved(string $queue, Job $job): void
    {
        $this->event($this->getQueueName($queue), new JobDeleted($job, $job->getRawBody()));
    }

    private function requeue(Job $job): void
    {
        $this->producer->produce(
            topic: $this->getQueueName($job->getQueue()),
            payload: $job->getRawBody(),
            delay: $job->getMessageTimestamp(),
            callback: fn() => $this->consumer->commit()
        );
    }

    private function getQueueName(?string $queue): string
    {
        return $queue ?? $this->defaultQueue;
    }

    protected function event($queue, $event): void
    {
        if (!$this->horizon) {
            return;
        }

        if ($this->container && $this->container->bound(Dispatcher::class)) {
            $this->container->make(Dispatcher::class)->dispatch(
                $event->connection($this->getConnectionName())->queue($queue)
            );
        }
    }

}
