#!/usr/bin/python2.7

### In order to run this script you have to install the following python package: 
### json,pymongo,TwitterAPI,nltk
### the download site for twitterAPI is https://github.com/geduldig/TwitterAPI
###
import json
import pymongo
from TwitterAPI import TwitterAPI
from pymongo import MongoClient
import time
import nltk
from nltk.tokenize import RegexpTokenizer
porter = nltk.PorterStemmer()

tokenizer = RegexpTokenizer(r'\w+|@\w+|#\w+')
stoplist=[eachline.strip() for eachline in open("/home/xiezhe/stoplist1.txt","r")]# stoplist

client=MongoClient('localhost',27017)
db=client['twitter']
collection=db['all_twitters']



consumer_key = "RylFXVaWC7l2R6C6pZGg2A"
consumer_secret = "O12NCnmLhBLghRtik3xuWUT6gTtq7psjg8mrfiho"
access_key = "370512026-mo5KjD1rv4OirABYlKY5uK84I2T309KLCPMbb3Ek"
access_secret = "rBzvqdXn7gZ2HXiK8fzzIKZBHQxTqwMY1ZbtlIGs"

api = TwitterAPI(consumer_key, consumer_secret, access_key, access_secret)

r=api.request('statuses/sample',{'language':'en'})

count=1

for item in r.get_iterator():
    # get the tweets
    text1=item["text"]
    # split the tweets into words
    temp=tokenizer.tokenize(text1)#split the sentence and only keep charactor,numbers and # @
    # remove stopwords 
    temp2=[val for val in temp if val.lower() not in set(stoplist)]#remove stopwords
    # get back to a sentence
    item["text2"]=" ".join(temp2)
    # add to mongodb
    collection.insert(item)
    print "Add another record! %d"%count
    count=count+1

    

