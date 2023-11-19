import logging
import json
import threading
from confluent_kafka import Producer 


class DurableProducer(Producer):
    def __init__(self, config):
        self.is_connected = True
        config['on_delivery'] = self.delivery_report
        config['stats_cb'] = self.stats_cb
        config['statistics.interval.ms'] = 1000
        super().__init__(config)
        self.lock = threading.Lock()

    def stats_cb(self, stats_json_str):
        stats_json = json.loads(stats_json_str)
        with self.lock:
            self.is_connected = all(broker_stats.get('state') == 'UP' for broker, broker_stats in stats_json.get('brokers', {}).items())
            # check if producer is connected
            if not self.is_connected:
                self.purge()
                self.flush()

    def produce(self, topic, key=None, value=None):
        with self.lock:
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
    
    def write_to_disk(self, topic, key, value):
        try:
            with open('failed_messages.log', 'a') as f:
                f.write(json.dumps({'topic': topic, 'key': key, 'value': value}) + '\n')
            logging.info("Written message to disk due to disconnection")
        except Exception as e:
            logging.error(f"Error writing message to disk: {e}")