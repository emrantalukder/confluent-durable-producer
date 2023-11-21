import logging
from durable_producer import DurableProducer


if __name__ == '__main__':
    producer = DurableProducer({
        'bootstrap.servers': 'localhost:9092',
        'client.id': 'durable-producer',
        'acks': 'all',
    })

    idx = 0
    while True:
        try:
            producer.produce('my-topic', key=f'{idx}', value='hello world')
            producer.poll()
            idx += 1
        except Exception as e:
            logging.error(e)