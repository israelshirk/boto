
from boto.ses.aconnection import ASESConnection

from twisted.internet import reactor
from twisted.internet.defer import Deferred, DeferredList
from twisted.web.client import Agent, HTTPConnectionPool

pool = HTTPConnectionPool(reactor, persistent=True)
pool.maxPersistentPerHost = 50
pool.cachedConnectionTimeout = 10
agent = Agent(reactor, pool=pool)

# Add txmysql support here!

def add_messages():
	connection = ASESConnection(aws_access_key_id=None, aws_secret_access_key=None, callback=None, errback=None, reactor=None)
	connection.send_email(..., my_callback, my_errback)

reactor.callLater(1, add_messages)

reactor.run()