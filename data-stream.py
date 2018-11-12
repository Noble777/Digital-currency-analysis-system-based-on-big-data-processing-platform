import argparse
import atexit
import logging
import json
import time

from kafka import KafkaProducer
from kafka.errors import KafkaError, KafkaTimeoutError
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

logger_format = '%(asctime)s - %(message)s'
logging.basicConfig(format=logger_format)
logger = logging.getLogger('stream-processing')
logger.setLevel(logging.INFO)


def shutdown_hook(producer):
    """
    a shutdown hook to be called before the shutdown
    """
    try:
        logger.info('Flushing pending messages to kafka, timeout is set to 10s')
        producer.flush(10)
        logger.info('Finish flushing pending messages to kafka')
    except KafkaError as kafka_error:
        logger.warn('Failed to flush pending messages to kafka, caused by: %s', kafka_error.message)
    finally:
        try:
            logger.info('Closing kafka connection')
            producer.close(10)
        except Exception as e:
            logger.warn('Failed to close kafka connection, caused by: %s', e.message)


def process_stream(stream, kafka_producer, target_topic):


if __name__ == '__main__':
	# Setup command line arguments.
	parser = argparse.ArgumentParser()
	parser.add_argument('source_topic')
	parser.add_argument('target_topic')
	parser.add_argument('kafka_broker')
	parser.add_argument('batch_duration', help='the batch duration in secs')

	# Parse arguments.
	args = parser.parse_args()
	source_topic = args.source_topic
	target_topic = args.target_topic
	kafka_broker = args.kafka_broker
	batch_duration = int(args.batch_duration)

	# Create SparkContext and StreamingContext.
	sc = SparkContext("local[2]", "AveragePrice")
	sc.setLogLevel('INFO')
	ssc = StreamingContext(sc, batch_duration)

	# Instantiate a kafka stream for processing.
	directKafkaStream = KafkaUtils.createDirectStream(ssc, [source_topic], { 'metadata.broker.list':kafka_broker })

	# Extract value from directKafkaStream (key, value) pair.
	stream = directKafkaStream.map(lambda msg:msg[1])

	# Instantiate a simple kafka producer.
	kafka_producer = KafkaProducer(bootstrap_servers=kafka_broker)

	process_stream(stream, kafka_producer, target_topic)

	# Setup shutdown hook.
	atexit.register(shutdown_hook, kafka_producer)

	ssc.start()
	ssc.awaitTermination()

