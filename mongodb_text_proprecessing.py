#!/usr/bin/python2.7
"""
in this text processing:
we first tokenize each tweet from mongodb
and then only keep the following symbols:character,number,@ and #
finally we remove all the stop word.
"""


from TwitterAPI import TwitterAPI
import json

import pymongo
from pymongo import MongoClient

import nltk
from nltk.tokenize import RegexpTokenizer

porter = nltk.PorterStemmer()

tokenizer = RegexpTokenizer(r'\w+|@\w+|#\w+')


client=MongoClient('localhost',27017)
db=client['twitter']
collection=db['tweet']

stoplist=[eachline.strip() for eachline in open("/home/xiezhe/stoplist1.txt","r")]
for num,tweet in enumerate(collection.find()):
    text1=tweet["text"]
    idd=tweet["_id"]
    temp=tokenizer.tokenize(text1)#split the sentence and only keep charactor,numbers and # @
    #temp=[token.lower() for token in temp]# lower case
    temp2=[val for val in temp if val.lower() not in set(stoplist)]#remove stopwords
    #temp3=[porter.stem(token) for token in temp2]#stemming
    collection.update({"_id":idd},{"$set":{"text2":" ".join(temp2)}},upsert=False)
    print "OK",num

#{"text2":{"$exists":False}}




    

