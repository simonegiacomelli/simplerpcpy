from simplerpcpy.distributed_conf import MqttConfiguration

distrib_conf = MqttConfiguration()
distrib_conf.broker_config.hostname = 'mqtt.flespi.io'
distrib_conf.broker_config.username = '6XidtbbkABE4iz07XCwRuBZPnlmQtLNqxTrOTsYjSrb0QZVDrfiKG6pShzyzbbNw'  # this is a flespi token
distrib_conf.broker_config.port = 1883