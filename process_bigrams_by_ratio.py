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

## To assign weight to each bigram 

# connect to the redis server and use the db 1 which stores all bigrams
# generate the count list for each bigram
r_server =redis.StrictRedis(host='127.0.0.1',db=1)
for j in range(len(data_count[0])):
        r_server.rpush(data_name[0][j],data_count[0][j])
for i in range(1,20):
    for j in range(len(data_name[i])):
        
        if data_name[i][j] not in set(r_server.keys()):
            for k in range(i):
                r_server.rpush(data_name[i][j],0)
                
        elif data_name[i][j] in set(r_server.keys()) and r_server.llen(data_name[i][j])<i: 
            for k in range(i-r_server.llen(data_name[i][j])):
                r_server.rpush(data_name[i][j],0)
                
        r_server.rpush(data_name[i][j],data_count[i][j])
    print "Load %d finished"%(i+1) 

### assign score to each bigram: how to design the weight
dataFramelist=[]
for i in range(20):
    temp=[]
    for j in range(len(data_count[i])):
         #print data_name[i][j]
            
         current=float(r_server.lrange(data_name[i][j],i,i)[0])
         if r_server.llen(data_name[i][j])<i+2:
                current=current+10
                last=10
         elif float(r_server.lrange(data_name[i][j],i+1,i+1)[0])==0:
                current=current+10
                last=10
         else:
                last=float(r_server.lrange(data_name[i][j],i+1,i+1)[0])
         ratio=current/last  
         temp_item={"id":data_name[i][j],"ratio":ratio,"idd":data_idd[i][j]}
         temp.append(temp_item)
    dataFramelist.append(DataFrame(temp))

## Find the representative twitters for each trigram
## Here we start to use redis, the in-memory database to be the data structure container because of the high flexibility and speed

#connect to the redis server database 0.
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
writer = ExcelWriter('outputbigramsRatio.xlsx')
for i in range(20):
    dataFramelist[i].sort(columns="ratio",ascending=0).to_excel(writer,'sheet%d'%(i+1))
    writer.save()
    print "Exporting sheet%d finished!"%(i+1)
