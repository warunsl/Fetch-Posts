# -*- coding: utf-8 -*-
from __future__ import unicode_literals
import sys
import json
import time
import datetime
import pymongo
import requests
import warnings
import webbrowser
from urlparse import parse_qs
from requests_oauthlib import OAuth1
from pymongo import MongoClient

# Uses oauth lib by Requests - https://github.com/requests/requests-oauthlib/
# Current release of requests_oauthlib (0.2.0) does not automatically encode the parameters in the call to OAuth1 to utf-8.
# Encodes explicitly all the strings that go into the OAuth1 constructor to utf-8 to circumvent the error 'ValueError: Only unicode objects are escapable.'
# The code does not currently check if the user has already authentcated our app. It redirects the user to the browser everytime to authenticate.

# Client(consumer) key and secret. Obtained by registering the app at dev.twitter.com
consumer_key = u"<consumer_key>"
consumer_secret = u"<consumer_secret>"

# OAuth endpoints
request_token_url = u"https://api.twitter.com/oauth/request_token"
authorize_url = u"https://api.twitter.com/oauth/authorize?oauth_token="
access_token_url = u"https://api.twitter.com/oauth/access_token"
streaming_url = "https://stream.twitter.com/1.1/statuses/filter.json?track=ios7%2C%20%27ios%207%27"
# streaming_url = "https://stream.twitter.com/1.1/statuses/filter.json?track=emmys%2C%20%27Emmy%20Awards%27%2C%20nph%2C%20%27Breaking%20Bad%27%2C%20BreakingBad%2C%20%40BreakingBad_AMC%2C%20Homeland%2C%20%40SHO_Homeland%2C%20%27Downton%20Abbey%27%2C%20DowntonAbbey%2C%20%40downtonabbey%2C%20%27Game%20of%20Thrones%27%2C%20%23GoT%2C%20GameOfThrones%2C%20%27House%20of%20Cards%27%2C%20%40houseofcards%2C%20HouseOfCards%2C%20%27Mad%20Men%27%2C%20%40MadMen_ABC%2C%20MadMen"

# Obtain the request tokens. These are used to redirect the user to the authorization URL to get the verifier PIN
oauth = OAuth1(consumer_key, client_secret=consumer_secret)
r = requests.post(url=request_token_url, auth=oauth)

credentials = parse_qs(r.content)
request_token = credentials['oauth_token'][0]
request_token = unicode(request_token, 'utf-8')
request_secret = credentials['oauth_token_secret'][0]
request_secret = unicode(request_secret, 'utf-8')

# Prompt the user to verify the app at the authorization URL and get the verifier PIN
authorize_url = authorize_url + request_token
print "Redirecting you to the browser to authorize...", authorize_url
webbrowser.open(authorize_url)
verifier = raw_input('Please enter your PIN : ')
verifier = unicode(verifier, 'utf-8')

# Once the user enters the PIN, we store the users access token and secret. This is used for further operations by this user.
oauth = OAuth1(consumer_key, client_secret=consumer_secret, resource_owner_key=request_token, resource_owner_secret=request_secret, verifier=verifier)
r = requests.post(url=access_token_url, auth=oauth)

credentials = parse_qs(r.content)
access_token = credentials.get('oauth_token')[0]
access_token = unicode(access_token, 'utf-8')
access_secret = credentials.get('oauth_token_secret')[0]
access_secret = unicode(access_secret, 'utf-8')

oauth = OAuth1(consumer_key, client_secret=consumer_secret, resource_owner_key=access_token, resource_owner_secret=access_secret)
r = requests.get(url=streaming_url, auth=oauth, stream = True)

# Create a connection to the mongodb instance. Passing no parameters will connect to default host (localhost) and port (27017)
connection = MongoClient()

# Store the database in a variable
db = connection.emmys

# Get the collection
collection = db.iosdata

t  = time.time()
t1 = time.time()
t0 = time.time()

counter = 0
minuteCounter = 0
for line in r.iter_lines() :
    if line :
        counter += 1
        t  = time.time()
        if t - t1 > 60:
            print("%s tweets per min @ %s"%((float(minuteCounter)/(t-t1)*60),datetime.datetime.fromtimestamp(t)))
            minuteCounter = 1
            t1 = t
        else:
            minuteCounter += 1
        # Insert each json as an entry in the mongodb collection
        entry = collection.insert(json.loads(line))
        # print json.loads(line)['text']