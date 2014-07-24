#!/usr/bin/python2.7
"""
drop testing collections
"""


from TwitterAPI import TwitterAPI
import json

import pymongo
from pymongo import MongoClient

import nltk
from nltk.tokenize import RegexpTokenizer



client=MongoClient('localhost',27017)
db=client['twitter']

for i in range(40):
    col_name='test%d'%(i+1)
    collection=db[col_name]
    collection.drop()








    

