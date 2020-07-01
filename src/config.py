from simplerpcpy.distributed_conf import MqttConfiguration

distrib_conf = MqttConfiguration()
bc = distrib_conf.broker_config
bc.manager_queue = 'example/addition1.0'
bc.hostname = 'mqtt.flespi.io'
bc.username = '6XidtbbkABE4iz07XCwRuBZPnlmQtLNqxTrOTsYjSrb0QZVDrfiKG6pShzyzbbNw'  # this is a flespi token
bc.password = None
bc.ssl = False # the ssl was giving some connection unstability
bc.port = 8883 if bc.ssl else 1883
