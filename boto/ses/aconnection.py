# Copyright (c) 2010 Mitch Garnaat http://garnaat.org/
# Copyright (c) 2011 Harry Marr http://hmarr.com/
#
# Permission is hereby granted, free of charge, to any person obtaining a
# copy of this software and associated documentation files (the
# "Software"), to deal in the Software without restriction, including
# without limitation the rights to use, copy, modify, merge, publish, dis-
# tribute, sublicense, and/or sell copies of the Software, and to permit
# persons to whom the Software is furnished to do so, subject to the fol-
# lowing conditions:
#
# The above copyright notice and this permission notice shall be included
# in all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS
# OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABIL-
# ITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT
# SHALL THE AUTHOR BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
# WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
# IN THE SOFTWARE.
import re
import urllib
import base64

from boto.connection import AWSAuthConnection, HTTPRequest, HTTPResponse
from boto.exception import BotoServerError
from boto.regioninfo import RegionInfo
import boto
import boto.jsonresponse
from boto.ses import exceptions as ses_exceptions
from connection import *
from urllib import urlencode

from pprint import pprint

from twisted.internet import reactor
from twisted.internet.defer import Deferred, DeferredList, succeed
from twisted.internet.protocol import Protocol
from twisted.web.client import Agent, HTTPConnectionPool
from twisted.web.http_headers import Headers
from twisted.web.iweb import IBodyProducer
from zope.interface import implements

class StringProducer(object):
    implements(IBodyProducer)

    def __init__(self, body):
        self.body = body
        self.length = len(body)

    def startProducing(self, consumer):
        consumer.write(self.body)
        return succeed(None)

    def pauseProducing(self):
        pass

    def stopProducing(self):
        pass

class ResponseReceiver(Protocol):
    def __init__(self, finished, callback, response, data):
        self.finished = finished
        self.body = ""
        self.callback = callback
        self.response = response
        self.data = data

    def dataReceived(self, bytes):
        self.body += bytes

    def connectionLost(self, reason):
        # print 'Finished receiving body:', reason.getErrorMessage()

        self.callback(self.response, self.body, self.data)

        self.finished.callback(None)

class ASESConnection(SESConnection):
    ResponseError = BotoServerError
    DefaultRegionName = 'us-east-1'
    DefaultRegionEndpoint = 'email.us-east-1.amazonaws.com'
    # DefaultRegionEndpoint = 'coast.dev'
    APIVersion = '2010-12-01'

    def __init__(self, aws_access_key_id=None, aws_secret_access_key=None,
                 is_secure=True, port=None, proxy=None, proxy_port=None,
                 proxy_user=None, proxy_pass=None, debug=0,
                 https_connection_factory=None, region=None, path='/',
                 security_token=None, validate_certs=True, agent=None, callback=None, errback=None, reactor=None):
        if not region:
            region = RegionInfo(self, self.DefaultRegionName,
                                self.DefaultRegionEndpoint)
        self.region = region
        SESConnection.__init__(self,
                                   aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key,
                                   is_secure=is_secure, port=port, proxy=proxy, proxy_port=proxy_port,
                                   proxy_user=proxy_user, proxy_pass=proxy_pass, debug=debug,
                                   https_connection_factory=None, path=path,
                                   security_token=security_token,
                                   validate_certs=validate_certs)
        self.agent = agent
        self.callback = callback
        self.errback = errback
        self.reactor = reactor

    def _make_request(self, action, params=None):
        """Make a call to the SES API.

        :type action: string
        :param action: The API method to use (e.g. SendRawEmail)

        :type params: dict
        :param params: Parameters that will be sent as POST data with the API
            call.
        """
        ct = 'application/x-www-form-urlencoded; charset=UTF-8'
        headers = {'Content-Type': ct}
        params = params or {}
        params['Action'] = action

        for k, v in params.items():
            if isinstance(v, unicode):  # UTF-8 encode only if it's Unicode
                params[k] = v.encode('utf-8')

        self.make_request(
            'POST',
            '/',
            headers=headers,
            data=urllib.urlencode(params)
        )

    def make_request(self, method, path, headers=None, data='', host=None,
                     auth_path=None, sender=None, override_num_retries=None,
                     params=None):
        """Makes a request to the server, with stock multiple-retry logic."""
        if params is None:
            params = {}
        self.http_request = self.build_base_http_request(method, path, auth_path,
                                                    params, headers, data, host)

        self.http_request.authorize(connection=self)

        bodyProducer = StringProducer(self.http_request.body)

        url = self.http_request.protocol + '://' + self.http_request.host

        url += self.http_request.path

        # Twisted sets this again - doing it twice results in a 400 Bad Request...
        del(self.http_request.headers['Content-Length'])

        for (title, value) in self.http_request.headers.iteritems():
            if type(value) == type("string"):
                value = [ value ]

                self.http_request.headers[title] = value

        # print self.http_request.method, url, Headers(self.http_request.headers), self.http_request.body

        d = self.agent.request(
            self.http_request.method, 
            url, 
            Headers(self.http_request.headers), 
            bodyProducer=bodyProducer
            )

        d.addCallback(self.http_request_callback, data)
        d.addErrback(self.http_request_errback, data)

    def http_request_callback(self, response, data):
        # print "http_request_callback"

        # pprint(response.__dict__)

        finished = Deferred()

        response.deliverBody(ResponseReceiver(finished, self.http_body_received_callback, response, data))

        return finished

    def http_body_received_callback(self, response, body, data):
        if response.code == 200:
            list_markers = ('VerifiedEmailAddresses', 'Identities',
                            'VerificationAttributes', 'SendDataPoints')
            item_markers = ('member', 'item', 'entry')

            try:
                e = boto.jsonresponse.Element(list_marker=list_markers,
                                              item_marker=item_markers)
                h = boto.jsonresponse.XmlHandler(e, None)
                h.parse(body)
                self.callback( e, data )

                return
            except Exception, e:
                # print e
                self.errback( self._handle_error(response, body), data )

                return
        else:
            # HTTP codes other than 200 are considered errors. Go through
            # some error handling to determine which exception gets raised,
            self.errback( self._handle_error(response, body), data )

            return

    def http_request_errback(self, failure, data):
        self.errback( "HTTP Error", data )

    def _handle_error(self, response, body):
        """
        Handle raising the correct exception, depending on the error. Many
        errors share the same HTTP response code, meaning we have to get really
        kludgey and do string searches to figure out what went wrong.
        """

        if "Address blacklisted." in body:
            # Delivery failures happened frequently enough with the recipient's
            # email address for Amazon to blacklist it. After a day or three,
            # they'll be automatically removed, and delivery can be attempted
            # again (if you write the code to do so in your application).
            ExceptionToRaise = ses_exceptions.SESAddressBlacklistedError
            exc_reason = "Address blacklisted."
        elif "Email address is not verified." in body:
            # This error happens when the "Reply-To" value passed to
            # send_email() hasn't been verified yet.
            ExceptionToRaise = ses_exceptions.SESAddressNotVerifiedError
            exc_reason = "Email address is not verified."
        elif "Daily message quota exceeded." in body:
            # Encountered when your account exceeds the maximum total number
            # of emails per 24 hours.
            ExceptionToRaise = ses_exceptions.SESDailyQuotaExceededError
            exc_reason = "Daily message quota exceeded."
        elif "Maximum sending rate exceeded." in body:
            # Your account has sent above its allowed requests a second rate.
            ExceptionToRaise = ses_exceptions.SESMaxSendingRateExceededError
            exc_reason = "Maximum sending rate exceeded."
        elif "Domain ends with dot." in body:
            # Recipient address ends with a dot/period. This is invalid.
            ExceptionToRaise = ses_exceptions.SESDomainEndsWithDotError
            exc_reason = "Domain ends with dot."
        elif "Local address contains control or whitespace" in body:
            # I think this pertains to the recipient address.
            ExceptionToRaise = ses_exceptions.SESLocalAddressCharacterError
            exc_reason = "Local address contains control or whitespace."
        elif "Illegal address" in body:
            # A clearly mal-formed address.
            ExceptionToRaise = ses_exceptions.SESIllegalAddressError
            exc_reason = "Illegal address"
        # The re.search is to distinguish from the
        # SESAddressNotVerifiedError error above.
        elif re.search('Identity.*is not verified', body):
            ExceptionToRaise = ses_exceptions.SESIdentityNotVerifiedError
            exc_reason = "Identity is not verified."
        elif "ownership not confirmed" in body:
            ExceptionToRaise = ses_exceptions.SESDomainNotConfirmedError
            exc_reason = "Domain ownership is not confirmed."
        else:
            # This is either a common AWS error, or one that we don't devote
            # its own exception to.
            ExceptionToRaise = self.ResponseError
            exc_reason = "unknown"

        return ExceptionToRaise(response.code, exc_reason, body)

