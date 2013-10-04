# -*- coding: utf-8 -*-
"""
Track keywords on Twitter using the public Streaming API

2013

"""
from __future__ import unicode_literals
import argparse
import sys
import json
import time
import datetime
import pymongo
import requests
import webbrowser
from urlparse import parse_qs
from requests_oauthlib import OAuth1, OAuth1Session
from pymongo import MongoClient


#
# API / OAuth management
#

def get_session(client_key,
                client_secret,
                resource_owner_key=None,
                resource_owner_secret=None):
    """Return an authenticated OAuth1Session object

       Followed the examples here:
       https://requests-oauthlib.readthedocs.org/en/latest/oauth1_workflow.html
    """
    if not resource_owner_key or not resource_owner_secret:
        resource_owner_key, resource_owner_secret = authorize(client_key, client_secret)
    return OAuth1Session(client_key,
                              client_secret=client_secret,
                              resource_owner_key=resource_owner_key,
                              resource_owner_secret=resource_owner_secret)

def authorize(consumer_key, consumer_secret):

    # Uses oauth lib by Requests - https://github.com/requests/requests-oauthlib/
    # Current release of requests_oauthlib (0.2.0) does not automatically encode the 
    #   parameters in the call to OAuth1 to utf-8.
    # Encodes explicitly all the strings that go into the OAuth1 constructor to utf-8 
    #   to circumvent the error 'ValueError: Only unicode objects are escapable.'
    # The code does not currently check if the user has already authentcated our app. 
    # It redirects the user to the browser everytime to authenticate.

    # OAuth endpoints
    request_token_url = u"https://api.twitter.com/oauth/request_token"
    authorize_url = u"https://api.twitter.com/oauth/authorize?oauth_token="
    access_token_url = u"https://api.twitter.com/oauth/access_token"

    # Obtain the request tokens. 
    # These are used to redirect the user to the authorization URL to get the verifier PIN
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

    # Once the user enters the PIN, we store the users access token and secret.
    # This is used for further operations by this user.
    oauth = OAuth1(consumer_key, 
                    client_secret=consumer_secret, 
                    resource_owner_key=request_token, 
                    resource_owner_secret=request_secret, 
                    verifier=verifier)
    r = requests.post(url=access_token_url, auth=oauth)
    credentials = parse_qs(r.content)
    access_token = credentials.get('oauth_token')[0]
    access_token = unicode(access_token, 'utf-8')
    access_secret = credentials.get('oauth_token_secret')[0]
    access_secret = unicode(access_secret, 'utf-8')
    return access_token, access_secret


#
# API wrappers
#

def track(twitter, keywords=[], user_ids=[]):
    """Iterator that yields tweets as dicts one at a time

        twitter: OAuth1Session object authenticated already
        keywords: a list of strings to track
        user_ids: a list of user_ids to track
    """

    # Prepare for GET request
    streaming_url = "https://stream.twitter.com/1.1/statuses/filter.json"

    # Documentation for filter params:
    #     https://dev.twitter.com/docs/streaming-apis/parameters
    params = {"replies": "all"}
    if keywords:
        params["track"] = keywords
    if user_ids:
        params["follow"] = user_ids

    # Create Request.get object
    r = twitter.get(url=streaming_url, params=params, stream = True)

    # Iterate over the request
    for line in r.iter_lines():
        if line :
            try:
                tweet = json.loads(line)
                yield tweet
            except ValueError:
                # Couldn't construct a valid tweet
                pass


# 
# Output functions
#

def get_db_collection(db_name, collection_name):
    """ Connect to the local Mongo daemon and 
        return an object pointing to the collection 

        db_name: string
        collection_name: string
    """

    # Create a connection to the mongodb instance. 
    # Passing no parameters will connect to default host (localhost) and port (27017)
    connection = MongoClient()

    # Store the database in a variable
    db = connection[db_name]

    # Get the collection
    collection = db[collection_name]

    return collection

def dump_to_mongo(tracker, collection): 
    """ Loop over the tweets in tracker and insert them into collection
       
        tracker: iterator returned from track()
        collection: pymongo object returned from get_db_collection()
    """

    t  = time.time()
    t1 = time.time()
    t0 = time.time()

    counter = 0
    minuteCounter = 0
    for tweet in tracker: 
        counter += 1
        t  = time.time()
        if t - t1 > 60:
            print("%s tweets per min @ %s"%((float(minuteCounter)/(t-t1)*60),datetime.datetime.fromtimestamp(t)))
            minuteCounter = 1
            t1 = t
        else:
            minuteCounter += 1

        # Use the numeric Tweet ID as primary key  
        tweet['_id'] = tweet['id_str']

        # Insert each json as an entry in the mongodb collection
        entry = collection.insert(tweet)
        
def dump_to_stdout(tracker, encoding='utf-16', tracer=0):
    """ Loop over tweets in tracker and print them to stdout 
        If tracer a non-zero integer, then the text of 
            every tracer-th tweet will be printed to stderr
    """

    t  = time.time()
    t1 = time.time()
    minuteCounter = 0
    tweets_per_min = -1

    for n, tweet in enumerate(tracker):
        j = json.dumps(tweet, encoding=encoding)
        print j
        minuteCounter += 1

        # Print tracer to stderr
        if tracer:
            if not n % tracer:

                # Calc tweets per minute
                t  = time.time()
                tweets_per_min = round(float(minuteCounter)/(t-t1)*60, 2)
                minuteCounter = 1
                t1 = t

                # This is lazy but catching unicode errors
                # is a pain in the neck
                try:
                    text = tweet.get('text', '').encode('utf-8', errors="replace")
                    username = tweet['user'].get('screen_name', '').encode('utf-8', errors="replace")
                except:
                    text = ''
                    username = ''

                for s in (unicode(n), 
                            u' ', 
                            unicode(tweets_per_min), 
                            u' ', 
                            username, 
                            u' ', 
                            text, u'\n'):
                    sys.stderr.write(s)


if __name__=="__main__":

    # Parse command line arguments
    parser = argparse.ArgumentParser()
    parser.add_argument('--clientkey', 
                            type=str,
                            default=u"",
                            help="Consumer Key")
    parser.add_argument('--clientsecret', 
                            type=str,
                            default=u"",
                            help="Consumer Secret")
    parser.add_argument('--resourcekey', 
                            type=str,
                            default=u"",
                            help="Access Token")
    parser.add_argument('--resourcesecret', 
                            type=str,
                            default=u"",
                            help="Access Token Secret")
    parser.add_argument('--keywords',
                            type=str,
                            default='',
                            help='Path to file with keywords, one per line')
    parser.add_argument('--userids',
                            type=str,
                            default='',
                            help='Path to file with user IDs, one per line')
    parser.add_argument('--tracer', 
                            type=int,
                            default=0,
                            help="How often to print a tweet to stderr")

    args = parser.parse_args()

    consumer_key = args.clientkey
    consumer_secret = args.clientsecret 
    access_token = args.resourcekey 
    access_token_secret = args.resourcesecret

    if not args.keywords and not args.userids:
        sys.stderr.write("Nothing to track! Please supply keywords or user IDs.\n")
        sys.exit(1)

    keywords = []
    if args.keywords:
        sys.stderr.write('\nParsing keyword file:\n')
        with open(args.keywords, 'rb') as f:
            for line in f:
                kw = line.strip()
                if kw:
                    keywords.append(kw)
                    sys.stderr.write('\t')
                    sys.stderr.write(kw)
                    sys.stderr.write('\n')

    user_ids = []
    if args.userids:
        sys.stderr.write('\nParsing user IDs file:\n')
        with open(args.userids, 'rb') as f:
            for line in f:
                uid = line.strip()
                if uid:
                    keywords.append(uid)
                    sys.stderr.write('\t')
                    sys.stderr.write(uid)
                    sys.stderr.write('\n')

    sys.stderr.write('\nAuthorizing tracker with Twitter...')
    sesh = get_session(consumer_key, 
                        consumer_secret, 
                        access_token, 
                        access_token_secret)
    stream = track(sesh, keywords, user_ids)
    sys.stderr.write('done!\n')

    sys.stderr.write('\nStarting tracker...\n')
    dump_to_stdout(stream, tracer=args.tracer)
