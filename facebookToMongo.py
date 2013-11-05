import json
import time
import urllib
import pymongo
import requests
import webbrowser
from pprint import pprint
from pymongo import MongoClient
from urlparse import parse_qs

app_id = u"<app_id>"
app_secret = u"<app_secret>"

authorize_url = u"https://www.facebook.com/dialog/oauth/?"
access_token_url = u"https://graph.facebook.com/oauth/access_token?"
search_url = u"https://graph.facebook.com/search?q=emmyawards&type=post&access_token="

authorization_parameters = {'redirect_uri': 'http://0.0.0.0:8000/',
                            'client_id': app_id,
                            'scope': 'read_stream, publish_stream'}
authorize_url = authorize_url + urllib.urlencode(authorization_parameters)

print "Redirecting you to the browser to authorize...", authorize_url
webbrowser.open(authorize_url)

#TO-DO : Explore reading from the socket to bypass this process
verifier = raw_input('Please enter your PIN : ')
verifier = unicode(verifier, 'utf-8')

authentication_parameters = {'redirect_uri': 'http://0.0.0.0:8000/',
                             'client_id': app_id,
                             'client_secret': app_secret,
                             'code': verifier}
r = requests.post(url=access_token_url +
                  urllib.urlencode(authentication_parameters))
credentials = parse_qs(r.content)
print credentials
access_token = credentials.get('access_token')[0]
access_token = unicode(access_token, 'utf-8')

r = requests.get(search_url+access_token)

try:
    json.loads(r.content)
except ValueError:
    sys.exit("Invalid JSON returned from the Facebook API")

currentPageJson = json.loads(r.content)

connection = MongoClient()
db = connection.emmys
collection = db.facebookdatanew

while(currentPageJson["data"]):
    # Printing the first status update of every page of 25 updates
    entry = collection.insert(currentPageJson["data"])
    #print currentPageJson["data"][0]["message"]
    nextPageUrl = currentPageJson["paging"]["next"]
    r = requests.get(nextPageUrl)
    print r.url
    currentPageJson = json.loads(r.content)
    if(currentPageJson["data"]):
        pass
    else:
        time.sleep(180)
        r = requests.get(search_url+access_token)
        currentPageJson = json.loads(r.content)