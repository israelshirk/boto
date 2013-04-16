
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

from twisted_queue import *

# This class is used to enable SSL...  Seems to just extend getContext to accept extra arguments that HTTP might require.
class WebClientContextFactory(ClientContextFactory):
    def getContext(self, hostname, port):
        return ClientContextFactory.getContext(self)

class EmailSender:

	def __init__(self, reactor):
		contextFactory = WebClientContextFactory()

		pool = HTTPConnectionPool(reactor, persistent=True)
		pool.maxPersistentPerHost = 20
		pool.cachedConnectionTimeout = 10
		https_agent = Agent(reactor, contextFactory, pool=pool)

		self.db_pool = txmysql.client.ConnectionPool("127.0.0.1", "test","test",
											database='test', num_connections=50,
											idle_timeout=120,
											connect_timeout=20)

		self.ses_connection = ASESConnection(callback=self.sending_email_succeeded, errback=self.sending_email_failed, reactor=reactor, agent=https_agent)

		self.sent_count = 0
		self.reactor = reactor

		self.reactor.callWhenRunning(self.get_email_info)

	def get_email_info( self ):
		d = self.db_pool.selectDb("test")
		d.addCallback(functools.partial(self.select_email, d))
		d.addCallback(functools.partial(self.got_email_content, d))

	def select_email( self, deferred, ignored ):
		return self.db_pool.runQuery(" SELECT subject, content FROM anniversary_2013_email_content WHERE id = 1 ")

	def got_email_content( self, deferred, data ):
		try:
			data = data[0]

			subject = data[0]
			content = data[1]
		except:
			self.reactor.stop()
			import sys; sys.exit(0)

		self.email_body = content
		self.email_subject = subject
		self.email_source = "noreply@mojo-themes.com"

		# self.reactor.stop()
		# import sys; sys.exit(0)

		message_queue = twisted_queue_receiver(self.send_email, "127.0.0.1", 5672, '/', "guest", "guest", queue="huge_email_address_queue", exchange="huge_email_address_queue", routing_key="huge_email_address_queue", rate_limit=65, no_ack=False, durable=True)

	def send_email( self, channel, raw_message ):
		if not (channel or raw_message):
			return

		channel.basic_ack(delivery_tag=raw_message.delivery_tag)

		try:
			message = json.loads(raw_message.content.body)
		except ValueError, e:
			print "Error parsing email", e

			return

		self.ses_connection.send_email(source=self.email_source, subject=self.email_subject, body=self.email_body, format='html', to_addresses=message[0])

	def sending_email_succeeded( self, e, data ):

		data = re.sub(r'.*\.member\.1=(.*[^&$]).*', r'\1', data )
		data = urllib.unquote( data )
		data = _mysql.escape_string( data )

		sys.stdout.write('.')

		self.db_pool.runOperation("INSERT INTO succeeded_emails (data) VALUES ('%s')" % (data))

		self.sent_count += 1

	def sending_email_failed( self, e, data ):
		data = re.sub(r'.*\.member\.1=(.*[^&$]).*', r'\1', data)
		e = _mysql.escape_string( str( e ) )
		data = _mysql.escape_string( data )

		sys.stdout.write(',')

		self.db_pool.runOperation("INSERT INTO failed_emails (e, data) VALUES ('%s', '%s')" % (e, data))

		#self._pool.runOperation("insert into example set data='%s'" % data)
		#print "failure", e

sender = EmailSender(reactor)

reactor.run()