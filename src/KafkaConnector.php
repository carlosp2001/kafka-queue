<?php

namespace CarlosPineda\KafkaQueue;

use Illuminate\Queue\Connectors\ConnectorInterface;

class KafkaConnector implements ConnectorInterface
{

    public function connect(array $config)
    {
        $conf = new \RdKafka\Conf();

        $conf->set('bootstrap.servers', $config['bootstrap_servers']);

        if (env('APP_ENV') !== 'local') {
            // Configuración de seguridad SASL/SCRAM-SHA-256
            $conf->set('security.protocol', $config['security.protocol']);
            $conf->set('sasl.mechanism', $config['sasl.mechanisms']);
            $conf->set('sasl.username', $config['sasl.username']);
            $conf->set('sasl.password', $config['sasl.password']);

        }
        // Configuración SSL/TLS
        // agregar ca crt
        $producer = new \RdKafka\Producer($conf);

        $conf->set('group.id', 'recipes-service');
        $conf->set('auto.offset.reset', 'latest');

        $consumer = new \RdKafka\KafkaConsumer($conf);

        return new KafkaQueue($consumer, $producer);
    }
}
