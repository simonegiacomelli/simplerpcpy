class BrokerConfig:
    hostname = 'mqtt.flespi.io'
    username = 'NaDoY3zFmGhB1z1eGAlXyLx48bsSgjVTjm68dtBoZGPO5GQA42nsDKnzLDfPM0xU'  # this is a flespi token
    password = None
    port = 1883
    ssl = False
    from gmqtt.mqtt.constants import MQTTv311, MQTTv50
    mqtt_version = MQTTv50


class MqttConfiguration:
    manager_book_keepint_interval = 1

    manager_queue = 'tig/beamng1'
    broker_config = BrokerConfig()

    message_timeout = 2.0
    prune_timeout = message_timeout * 2 + 1
    presence_interval_seconds: float = message_timeout


distrib_conf = MqttConfiguration()
distrib_conf_test = MqttConfiguration()

distrib_conf_test.manager_queue = 'tig/test/beamng1'
