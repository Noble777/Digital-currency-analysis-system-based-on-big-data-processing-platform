import json
import math

from unittest.mock import MagicMock
from pyspark import SparkContext
from pyspark.streaming import StreamingContext

from time import sleep

data_stream_module = __import__("data-stream")

topic = 'test_topic'

# 42000 / 3 = 14000
test_input = [
    json.dumps({'Timestap': '1526900000001', 'Symbol': 'BTC-USD', 'LastTradePrice':'10000'}),
    json.dumps({'Timestap': '1526900000001', 'Symbol': 'BTC-USD', 'LastTradePrice':'12000'}),
    json.dumps({'Timestap': '1526900000001', 'Symbol': 'BTC-USD', 'LastTradePrice':'20000'}),
]


def _make_dstream_helper(sc, ssc, test_input):



def test_data_stream(sc, ssc, topic):



if __name__ == '__main__':
	sc = SparkContext('local[2]', 'local-testing')
	ssc = StreamingContext(sc, 1)

	test_data_stream(sc, ssc, topic)