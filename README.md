# kafka-queue
Laravel Kafka Queue connection with Horizon support

## Installation

### 1. Composer
```shell
composer require laravel-tool/kafka-queue
```

### 2. Add to config/queue.php
```php
'kafka' => [
    'driver' => 'kafka',
    'broker_list' => env('KAFKA_BROKER_LIST', 'kafka:9092'),
    'queue' => env('KAFKA_QUEUE', 'default'),
    'heartbeat_ms' => env('KAFKA_HEARTBEAT', 5000),
    'group_name' => env('KAFKA_QUEUE_GROUP', 'default'),
    'producer_timeout_ms' => 1000,
    'consumer_timeout_ms' => 3000,
    'queue_disable_length' => false, // Disable this if trouble with performance
    'horizon' => true, // Support for Laravel Horizon
    'after_commit' => false,
],
```