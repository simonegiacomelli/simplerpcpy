
class BrokerConfig:
    hostname = ''  # for example: 'mqtt.flespi.io'
    username = ''  # for example a flespi token
    password = None
    port = 1883
    ssl = False
    from gmqtt.mqtt.constants import MQTTv50
    mqtt_version = MQTTv50


class MqttConfiguration:
    manager_book_keeping_interval = 1

    manager_queue = 'tig/beamng1'
    broker_config = BrokerConfig()

    message_timeout = 2.0
    prune_timeout = message_timeout * 2 + 1
    presence_interval_seconds: float = message_timeout
