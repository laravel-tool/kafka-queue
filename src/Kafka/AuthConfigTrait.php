<?php

namespace LaravelTool\KafkaQueue\Kafka;

use RdKafka\Conf as KafkaConfig;

trait AuthConfigTrait
{
    public function authConfig(KafkaConfig &$kafkaConfig, array $config): void
    {
        if (!empty($config['auth']) && $config['auth']['enable']) {
            $kafkaConfig->set('sasl.mechanisms', $config['auth']['mechanism']);
            $kafkaConfig->set('sasl.username', $config['auth']['username']);
            $kafkaConfig->set('sasl.password', $config['auth']['password']);
            if ($config['auth']['ssl_ca_location']) {
                $kafkaConfig->set('ssl.ca.location', $config['auth']['ssl_ca_location']);
            }
        }
    }
}
