import logging
from logging.handlers import RotatingFileHandler
import json
from confluent_kafka import Producer 


class DurableProducer(Producer):
    def __init__(self, config):
        # set up logger
        clientid = config['client.id'] or 'durable-producer'
        self.logger = logging.getLogger(config['client.id'])
        self.logger.setLevel(logging.INFO)
        # TODO: add log rotation options from config
        handler = RotatingFileHandler(f'logs/{clientid}.log', maxBytes=1000, backupCount=5)
        self.logger.addHandler(handler)

        self.is_connected = True
        config['on_delivery'] = self.delivery_report
        config['stats_cb'] = self.stats_cb
        config['statistics.interval.ms'] = 1000
        super().__init__(config)

    def stats_cb(self, stats_json_str):
        stats_json = json.loads(stats_json_str)
        self.is_connected = all(broker_stats.get('state') == 'UP' for broker, broker_stats in stats_json.get('brokers', {}).items())
        # purge in-flight messages if disconnected from brokers:
        if not self.is_connected:
            self.purge()
            self.flush()

    def produce(self, topic, key=None, value=None):
        if self.is_connected:
            super().produce(topic, key=key, value=value, callback=self.delivery_report)
        else:
            self.write_to_disk(topic, key, value)

    # delivery callback
    def delivery_report(self, err, msg):
        if err:
            print('Message delivery failed: {}'.format(err))
            self.write_to_disk(msg.topic(), msg.key().decode(), msg.value().decode())
        else:
            print('Message delivered to {} [{}]\t {} {}'.format(msg.topic(), msg.partition(), msg.key(), msg.value()))
    
    # write message to disk
    def write_to_disk(self, topic, key, value):
        try:
            self.logger.info(json.dumps({'topic': topic, 'key': key, 'value': value}))
        except Exception as e:
            logging.error(f"Error writing message to disk: {e}")