# This adds the cloudfront servers' ip addresses into Memcache.
# Memcache Key: cloudfront_server_addresses
# Format:
# {
# 	'subnet': ['server', 'server', 'server', 'server', ...], 
# 	...
# }

import sys
from pprint import pprint
import time

from twisted.internet.defer import inlineCallbacks
from twisted.internet import reactor
from twisted.internet.protocol import ClientCreator
from twisted.python import log

from txamqp.protocol import AMQClient
from txamqp.client import TwistedDelegate

import txamqp.spec

class twisted_queue_receiver:

	@inlineCallbacks
	def gotConnection(self, conn, username, password):
		print "Connected to broker."
		yield conn.authenticate(username, password)

		print "Authenticated. Ready to receive messages"
		self.channel = yield conn.channel(1)
		yield self.channel.channel_open()

		yield self.channel.basic_qos(prefetch_count=self.prefetch_count)

		yield self.channel.queue_declare(queue=self.queue, durable=self.durable, exclusive=False, auto_delete=False)
		yield self.channel.exchange_declare(exchange=self.exchange, type="direct", durable=self.durable, auto_delete=False)

		yield self.channel.queue_bind(queue=self.queue, exchange=self.exchange, routing_key=self.routing_key)

		yield self.channel.basic_consume(queue=self.queue, no_ack=self.no_ack, consumer_tag=self.routing_key)

		self.queue = yield conn.queue(self.routing_key)

		self.getMessages()

	@inlineCallbacks
	def getMessages(self):
		"""
		def RateLimited(maxPerSecond):
			minInterval = 1.0 / float(maxPerSecond)
			def decorate(func):
				lastTimeCalled = [0.0]
				def rateLimitedFunction(*args,**kargs):
					elapsed = time.time() - lastTimeCalled[0]
					leftToWait = minInterval - elapsed
					if leftToWait>0:
						time.sleep(leftToWait)
					ret = func(*args,**kargs)
					lastTimeCalled[0] = time.time()
					return ret
				return rateLimitedFunction
			return decorate
		"""

		yield

		elapsed = time.time() - self.last_time_called
		left_to_wait = self.min_interval - elapsed

		if left_to_wait > 0:
			yield reactor.callLater(left_to_wait, self.getMessages)
		else:
			self.last_time_called = time.time()

			message = yield self.queue.get()
			self.callback(self.channel, message)

			elapsed = time.time() - self.last_time_called
			left_to_wait = self.min_interval - elapsed

			if left_to_wait < 0:
				left_to_wait = 0

			#print "left_to_wait: ", left_to_wait

			yield reactor.callLater(left_to_wait*1.01, self.getMessages)

	def __init__(self, callback, host, port, vhost, username, password, queue, exchange, routing_key, durable=False, rate_limit=5, prefetch_count=150, no_ack=False):
		import sys

		spec = "amqp0-8.stripped.rabbitmq.xml"

		self.queue = queue
		self.exchange = exchange
		self.routing_key = routing_key
		self.callback = callback
		self.durable = durable
		self.rate_limit = rate_limit

		self.min_interval = 1.0 / float(self.rate_limit)
		self.last_time_called = time.time()

		self.prefetch_count = prefetch_count

		self.no_ack = no_ack

		spec = txamqp.spec.load(spec)

		delegate = TwistedDelegate()

		d = ClientCreator(reactor, AMQClient, delegate=delegate, vhost=vhost, spec=spec).connectTCP(host, port)

		d.addCallback(self.gotConnection, username, password)

		def whoops(err):
			if reactor.running:
				log.err(err)
				reactor.stop()

		d.addErrback(whoops)

class twisted_queue_sender:

	@inlineCallbacks
	def gotConnection(self, conn, username, password):
		print "Connected to broker."
		yield conn.authenticate(username, password)

		print "Authenticated. Ready to send messages"
		self.channel = yield conn.channel(1)
		yield self.channel.channel_open()

		yield self.channel.queue_declare(queue=self.queue, durable=self.durable, exclusive=False, auto_delete=False)
		yield self.channel.exchange_declare(exchange=self.exchange, type="direct", durable=self.durable, auto_delete=False)

		yield self.channel.queue_bind(queue=self.queue, exchange=self.exchange, routing_key=self.routing_key)

		yield self.callback(self.channel)

		yield self.channel.channel_close()

		#channel0 = yield conn.channel(0)
		#yield channel0.channel_close()
		#reactor.stop()


	@inlineCallbacks
	def put(self, msg):
		yield self.queue.put(msg)

	def __init__(self, callback, host, port, vhost, username, password, queue, exchange, routing_key, durable=False):
		import sys

		spec = "amqp0-8.stripped.rabbitmq.xml"

		self.exchange = exchange
		self.routing_key = routing_key
		self.durable = durable
		self.callback = callback
		self.queue = queue

		spec = txamqp.spec.load(spec)

		delegate = TwistedDelegate()

		d = ClientCreator(reactor, AMQClient, delegate=delegate, vhost=vhost, spec=spec).connectTCP(host, port)

		d.addCallback(self.gotConnection, username, password)

		def whoops(err):
			if reactor.running:
				log.err(err)
				reactor.stop()

		d.addErrback(whoops)


