#!/usr/bin/python2.7
import json
import pandas as pd
from pandas import DataFrame,Series
from pandas import ExcelWriter
import pymongo
from pymongo import MongoClient
import bson
import redis

#Connect to mongdb  
client=MongoClient('localhost',27017)
db=client['twitter']
collection=db['all_twitters']

# Read the stoplist which just contains a few word but this process will be removed soon
# because all the preprocessing should be done in the "import data process"
stoplist=[eachline.strip() for eachline in open("/home/xiezhe/stoplist2.txt","r")]

## Read the data file which are exported from mongodb
data=[]
for i in range(20):
    temp_collection=db['bigramT%d'%(i+1)]
    temp_list=list(temp_collection.find())
    data.append(temp_list)
    print 'reading data bigramT%d finished'%(i+1)
   
## Data cleaning: remove bigrams with frequency of 1, bigrams with any element in the stopword set
## and bigrams with any element's length less than 2
for i in range(20):
    data[i]=[value for value in data[i] if value["value"]["count"]>1]
    data[i]=[value for value in data[i] if len(value["_id"].split())==2]
    data[i]=[value for value in data[i] if value["_id"].split()[0] not in set(stoplist) and value["_id"].split()[1] not in set(stoplist)]
    data[i]=[value for value in data[i] if len(value["_id"].split()[0])>2 and len(value["_id"].split()[1])>2]
    
data_count=[]
data_name=[]
data_idd=[]

for i in range(20):
    data_count.append([val["value"]["count"] for val in data[i]])
    data_name.append([val["_id"] for val in data[i]])
    data_idd.append([val["value"]["idd"] for val in data[i]])

print "Data loading into dataFrame finished!"


dataFramelist=[]
for i in range(20):
    temp=[]
    for j in range(len(data_count[i])):
         #print data_name[i][j] 
         temp_item={"id":data_name[i][j],"count":data_count[i][j],"idd":data_idd[i][j]}
         temp.append(temp_item)
    dataFramelist.append(DataFrame(temp))

## Construct a tweet list sorted by scores
## Here we start to use redis, the in-memory database to be the data structure container because of the high flexibility and speed

#connect to the redis server database 0.
r_server =redis.StrictRedis(host='127.0.0.1',db=0)
r_server2=redis.StrictRedis(host='127.0.0.1',db=5)
#flush database since db_0 db_5 just a temp database or a one-time container

r_server.flushdb()
r_server2.flushdb()

tweets_data_Frame_list=[]
for i in range(20):
    
    temp=dataFramelist[i]
    # assign a score to each _id which the trigram comes from and the score is the appearance time of one tweet
    for j in range(len(temp)):
       for k in range(len(dataFramelist[i]["idd"].loc[j])):
           r_server.incr(dataFramelist[i]["idd"].loc[j][k])
           r_server2.rpush(dataFramelist[i]["idd"].loc[j][k],dataFramelist[i]["id"].loc[j])
    
    
    tweets_list=[]
    
    # According to the _id, find the corresponding tweet in the mongodb and then add all the found tweets to the dataFrame
    for key in r_server.keys():
        temp_str=key
        temp_count=int(r_server.get(key))
        temp=collection.find({"_id":bson.objectid.ObjectId(str(temp_str))})
        # we must use the ObjectId() class from bson package to construct a _id object.
        temp_text=temp.next()["text"]
        temp_list=[] 
        for gram in r_server2.lrange(key,0,-1):
             temp_list.append(gram)
        
        temp_gram=",".join(temp_list)         

        tweets_list.append({"tweet":temp_text,"count":temp_count,"grams":temp_gram})
        
    
    tweets_data_Frame_list.append(DataFrame(tweets_list))
    

    print "Obtain the tweet sheet%d finished"%(i+1)
    # here we must clear up the redis databse in order to calculate for the next dataFrame
    r_server.flushdb()
    r_server2.flushdb()

# export the data into excel file
writer = ExcelWriter('outputbigramTweetList.xlsx')
for i in range(20):
    tweets_data_Frame_list[i].sort(columns="count",ascending=0).to_excel(writer,'sheet%d'%(i+1))
    writer.save()
    print "Exporting sheet%d finished!"%(i+1)
