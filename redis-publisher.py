import argparse
import atexit
import logging
import redis

from kafka import KafkaConsumer

logger_format = '%(asctime)s - %(message)s'
logging.basicConfig(format=logger_format)
logger = logging.getLogger('redis-publisher')
logger.setLevel(logging.INFO)

