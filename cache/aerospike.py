import aerospike
from utils.log import logger_instance

logger = logger_instance


class AerospikeClient(object):
    config = {
        'hosts': [('127.0.0.1', 3000)],
        'policies': {
            'timeout': 1000  # milliseconds
        }
    }

    def __init__(self, namespace='test', set='gamble'):
        self.client = aerospike.client(self.config).connect()
        self.namespace = namespace
        self.set = set

    def put(self, key, value, ttl=0):
        main_key = (self.namespace, self.set, key)
        logger.debug("Aero Key :{}, Value: {}".format(main_key, value))
        val_dict = {}
        val_dict['value'] = value
        return self.client.put(main_key, val_dict, meta={'ttl': ttl})

    def get(self, key):
        main_key = (self.namespace, self.set, key)
        logger.debug("Aero Key :{}".format(main_key))
        a, b, c = self.client.get(main_key)
        return c['value']


def get_aerospike_client():
    a = AerospikeClient()
    return a


def get_ltp_key(instrument):
    key = str(instrument)
    return key


aero_client = get_aerospike_client()
