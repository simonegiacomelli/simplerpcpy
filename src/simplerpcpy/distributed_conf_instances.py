from simplerpcpy.distributed_conf import MqttConfiguration

distrib_conf = MqttConfiguration()
distrib_conf_test = MqttConfiguration()

distrib_conf_test.manager_queue = 'tig/test/beamng1'
