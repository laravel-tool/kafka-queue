<?php

namespace LaravelTool\KafkaQueue;

use Illuminate\Support\ServiceProvider as BaseServiceProvider;

class ServiceProvider extends BaseServiceProvider
{
    public function boot(): void
    {
        $this->app['queue']->addConnector('kafka', function () {
            return new Connector();
        });
    }
}
