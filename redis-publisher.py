import argparse
import atexit
import logging
import redis

from kafka import KafkaConsumer

logger_format = '%(asctime)s - %(message)s'
logging.basicConfig(format=logger_format)
logger = logging.getLogger('redis-publisher')
logger.setLevel(logging.DEBUG)


def shutdown_hook(kafka_consumer):
	"""
	A shutdown hook to be called before the shutdown.
	"""
	try:
		kafka_consumer.close(10)
	except Exception as e:
		logger.warn('Failed to close kafka consumer for %s', e)

