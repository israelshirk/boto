
from boto.ses.aconnection import ASESConnection

from twisted.internet import reactor
from twisted.internet.defer import Deferred, DeferredList
from twisted.web.client import Agent, HTTPConnectionPool
from twisted.internet.ssl import ClientContextFactory
import boto
import txmysql.client
import functools
import _mysql
import json
import re
import urllib
import sys
from txamqp.content import Content

import pika

from twisted_queue import *

# This class is used to enable SSL...  Seems to just extend getContext to accept extra arguments that HTTP might require.
class WebClientContextFactory(ClientContextFactory):
    def getContext(self, hostname, port):
        return ClientContextFactory.getContext(self)

class EmailQueuer:

	def __init__(self, reactor):

		self.db_pool = txmysql.client.ConnectionPool("127.0.0.1", "test","test",
											database='test', num_connections=50,
											idle_timeout=120,
											connect_timeout=20)

		self.reactor = reactor

		credentials = pika.PlainCredentials("guest", "guest")
		connection = pika.BlockingConnection(pika.ConnectionParameters("127.0.0.1", 5672, '/', credentials))

		self.channel = connection.channel()

		self.channel.exchange_declare(exchange="huge_email_address_queue", type='direct', durable=True, auto_delete=False)

		self.reactor.callWhenRunning(self.set_up_db)

	def rabbit_connected( self, channel ):
		self.channel = channel

		self.set_up_db()

	def set_up_db( self ):
		d = self.db_pool.selectDb("test")
		d.addCallback(functools.partial(self.select_email, 0))
		d.addCallback(functools.partial(self.got_email_content, d))

	def select_email( self, deferred, ignored ):
		return self.db_pool.runQuery(" SELECT email FROM anniversary_2013_emails_mojo ")

	def got_email_content( self, deferred, data ):
		for address in data:
			print address

			self.channel.basic_publish(exchange="huge_email_address_queue", routing_key="huge_email_address_queue", body=json.dumps(address))



sender = EmailQueuer(reactor)

reactor.run()