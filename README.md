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
    'auth' => [
        'enable' => env('KAFKA_AUTH_ENABLE', false),
        'mechanism' => env('KAFKA_AUTH_MECHANISM', 'PLAIN'),
        'username' => env('KAFKA_AUTH_USERNAME'),
        'password' => env('KAFKA_AUTH_PASSWORD'),
        'ssl_ca_location' => env('KAFKA_AUTH_SSL_CA_LOCATION'),
    ],
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

## Caution
> It is not recommended to use it with deferred tasks due to the way Kafka works. 
> When a deferred task reaches the execution time, it is again placed at the end 
> of the queue and so on until the moment when it is allowed to be executed.