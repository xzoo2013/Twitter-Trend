#!/usr/bin/python2.7

import json
import pandas as pd
from pandas import DataFrame
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
    #data.append([json.loads(line) for line in open('/home/xiezhe/exportdata/exportdir/bigramT%d.json'%(i+1))])
print "Reading data finished!"
## here we extract data into three list of list, each list looks like[[],[],[],.......]   
data_count=[]
data_name=[]
data_idd=[]
for i in range(20):
    data_count.append([val["value"]["count"] for val in data[i]])
    data_name.append([val["_id"] for val in data[i]])
    data_idd.append([val["value"]["idd"] for val in data[i]])

print "Seperating the data into three list finished!"

## Data cleaning: remove bigrams with frequency of 1, bigrams with any element in the stopword set
## and bigrams with any element's length less than 2
dataFramelist=[]
for j in range(20):
    datalist=[]
    for i in range(len(data_count[j])):
        if len(data_name[j][i].split())==2:
            if data_name[j][i].split()[0] not in set(stoplist) and data_name[j][i].split()[1] not in set(stoplist):
                  if len(data_name[j][i].split()[0])>2 and len(data_name[j][i].split()[1])>2:
                          datalist.append({"id":data_name[j][i],"value":data_count[j][i],"idd":data_idd[j][i]})
                
    datalist=[val for val in datalist if val["value"]>1]
    
                
    df=DataFrame(datalist)
    dataFramelist.append(df)
print "Data loading into dataFrame finished!"

## Find the representative twitters for each trigram
## Here we start to use redis, the in-memory database to be the data structure container because of the high flexibility and speed

#connect to the redis server.
r_server =redis.StrictRedis(host='127.0.0.1',db=0)
r_server.flushdb()

for i in range(20):
    
    temp=dataFramelist[i]
    # assign a score to each _id which the trigram comes from and the score is the appearance time of one tweet
    for j in range(len(temp)):
       for k in range(len(dataFramelist[i]["idd"].loc[j])):
           r_server.incr(dataFramelist[i]["idd"].loc[j][k])
    # construct a ordered set in which the key is the trigram and the element is the representativ tweet' _id and score is the appearance time      
    for j in range(len(temp)):
       for k in range(len(dataFramelist[i]["idd"].loc[j])):
           r_server.zadd(dataFramelist[i]["id"].loc[j],int(r_server.get(dataFramelist[i]["idd"].loc[j][k])),dataFramelist[i]["idd"].loc[j][k]) 
    
    tweets_list=[]
    temp_list=[] # we first just consider one representative tweet but in order to increase the number of repres.. we use a list here
    # According to the _id, find the corresponding tweet in the mongodb and then add all the found tweets to the dataFrame
    for j in range(len(temp)):
        temp_str=r_server.zrange(dataFramelist[i]["id"].loc[j],-1,-1)[0]
        temp=collection.find({"_id":bson.objectid.ObjectId(str(temp_str))})
        # we must use the ObjectId() class from bson package to construct a _id object.
        text=temp.next()["text"]
        temp_list.append(text)
        #print text
        tweets_list.append(text) 

    dataFramelist[i]["tweet"]=tweets_list

    print "Obtain the representative tweets for sheet%d finished"%(i+1)
    # here we must clear up the redis databse in order to calculate for the next dataFrame
    r_server.flushdb()

# export the data into excel file
writer = ExcelWriter('outputbigrams.xlsx')
for i in range(20):
    dataFramelist[i].sort(columns="value",ascending=0).to_excel(writer,'sheet%d'%(i+1))
    writer.save()
    print "Exporting sheet%d finished!"%(i+1)
