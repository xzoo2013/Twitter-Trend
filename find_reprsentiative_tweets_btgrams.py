#!/usr/bin/python2.7

import json
import pandas as pd
from pandas import DataFrame,Series
from pandas import ExcelWriter
import pymongo
from pymongo import MongoClient
import bson
import redis
import numpy
import editdist

#Connect to mongdb  
client=MongoClient('localhost',27017)
db=client['twitter']
collection=db['all_twitters']
# Read the stoplist which just contains a few word but this process will be removed soon
# because all the preprocessing should be done in the "import data process"
stoplist=[eachline.strip() for eachline in open("/home/xiezhe/stoplist2.txt","r")]

## Read the data file which are exported from mongodb
data=[]
for i in range(5):
    temp_collection=db['bigramT%d'%(i+1)]
    temp_list=list(temp_collection.find())
    data.append(temp_list)
    print 'reading data bigramT%d finished'%(i+1)

data2=[]
for i in range(5):
    temp_collection2=db['unigramT%d'%(i+1)]
    temp_list2=list(temp_collection2.find())
    data2.append(temp_list2)
    #print temp_list2
    print 'reading data unigramT%d finished'%(i+1)

data3=[]
for i in range(5):
    temp_collection3=db['trigramT%d'%(i+1)]
    temp_list3=list(temp_collection3.find())
    data3.append(temp_list3)
    print 'reading data trigramT%d finished'%(i+1)

## Data cleaning: remove bigrams with frequency of 1, bigrams with any element in the stopword set
## and bigrams with any element's length less than 2
for i in range(5):
    data[i]=[value for value in data[i] if value["value"]["count"]>3]
    data[i]=[value for value in data[i] if len(value["_id"].split())==2]
    data[i]=[value for value in data[i] if value["_id"].split()[0] not in set(stoplist) and value["_id"].split()[1] not in set(stoplist)]
    data[i]=[value for value in data[i] if len(value["_id"].split()[0])>2 and len(value["_id"].split()[1])>2]
for i in range(5):
    data2[i]=[value for value in data2[i] if value["value"]["count"]>3]
    data2[i]=[value for value in data2[i] if value["_id"] not in set(stoplist)]
    data2[i]=[value for value in data2[i] if len(value["_id"])>2 ]

for i in range(5):
    data3[i]=[value for value in data3[i] if value["value"]["count"]>2]
    data3[i]=[value for value in data3[i] if len(value["_id"].split())==3]
    data3[i]=[value for value in data3[i] if value["_id"].split()[0] not in set(stoplist) and value["_id"].split()[1] not in set(stoplist) and value["_id"].split()[2] not in set(stoplist)]
    data3[i]=[value for value in data3[i] if len(value["_id"].split()[0])>2 and len(value["_id"].split()[1])>2 and len(value["_id"].split()[2])>2]
# seperate the data into a structured form   1
data_count=[]
data_name=[]
data_idd=[]

for i in range(5):
    data_count.append([val["value"]["count"] for val in data[i]])
    data_name.append([val["_id"] for val in data[i]])
    data_idd.append([val["value"]["idd"] for val in data[i]])

print "Data loading into dataFrame finished!"


dataFramelist=[]
for i in range(5):
    temp=[]
    for j in range(len(data_count[i])):
        #print data_name[i][j] 
        temp_item={"id":data_name[i][j],"count":data_count[i][j],"idd":data_idd[i][j]}
        temp.append(temp_item)
    dataFramelist.append(DataFrame(temp))
# seperate the data into a structured form   2
data_count2=[]
data_name2=[]
data_idd2=[]

for i in range(5):
    data_count2.append([val["value"]["count"] for val in data2[i]])
    data_name2.append([val["_id"] for val in data2[i]])
    data_idd2.append([val["value"]["idd"] for val in data2[i]])

print "Data loading into dataFrame finished!"


dataFramelist2=[]
for i in range(5):
    temp=[]
    for j in range(len(data_count2[i])):
         #print data_name[i][j] 
         temp_item={"id":data_name2[i][j],"count":data_count2[i][j],"idd":data_idd2[i][j]}
         temp.append(temp_item)
    dataFramelist2.append(DataFrame(temp))
# seperate the data into a structured form   3
data_count3=[]
data_name3=[]
data_idd3=[]

for i in range(5):
    data_count3.append([val["value"]["count"] for val in data3[i]])
    data_name3.append([val["_id"] for val in data3[i]])
    data_idd3.append([val["value"]["idd"] for val in data3[i]])

print "Data loading into dataFrame finished!"


dataFramelist3=[]
for i in range(5):
    temp=[]
    for j in range(len(data_count3[i])):
         #print data_name[i][j] 
         temp_item={"id":data_name3[i][j],"count":data_count3[i][j],"idd":data_idd3[i][j]}
         temp.append(temp_item)
    dataFramelist3.append(DataFrame(temp))


##################################################################################################down
## Construct a tweet list sorted by scores
## Here we start to use redis, the in-memory database to be the data structure container because of the high flexibility and speed

#connect to the redis server database 0.
r_server =redis.StrictRedis(host='127.0.0.1',db=0)

#flush database since db_0 db_5 db_6 just a temp database or a one-time container

r_server.flushdb()

tweets_data_Frame_list=[]
for i in range(5):
    
    temp=dataFramelist[i]# id ,count ,idd
    temp2=dataFramelist2[i]
    temp3=dataFramelist3[i]
    
    # assign a score to each _id which the trigram comes from and the score is the appearance time of one tweet
    for j in range(len(temp)):
       for k in range(len(dataFramelist[i]["idd"].loc[j])):
           r_server.zadd(dataFramelist[i]["idd"].loc[j][k],dataFramelist[i]["count"].loc[j],dataFramelist[i]["id"].loc[j])
    
    for j in range(len(temp3)):
       for k in range(len(dataFramelist3[i]["idd"].loc[j])):
           r_server.zadd(dataFramelist3[i]["idd"].loc[j][k],dataFramelist3[i]["count"].loc[j],dataFramelist3[i]["id"].loc[j])
    
    tweets_list=[]
    
    # According to the _id, find the corresponding tweet in the mongodb and then add all the found tweets to the dataFrame
    for key in r_server.keys():
        temp_str=key
        #temp_count=int(r_server.get(key))
        temp=collection.find({"_id":bson.objectid.ObjectId(str(temp_str))})
        # we must use the ObjectId() class from bson package to construct a _id object.
        temp_text=temp.next()["text"]
        temp_list=[]
        temp_count_list=[]
        temp_co_unig=0
        temp_co_big=0
        temp_co_trig=0
        lenkey=r_server.zcard(key)
        for p in range(r_server.zcard(key)):
            gram=r_server.zrange(key,-1-p,-1-p,withscores=True)
            #print gram
            if len(gram[0][0].split())==1:
                temp_co_unig=temp_co_unig+1
                if temp_co_unig<4:
                    temp_list.append(gram[0][0])
                    temp_count_list.append(gram[0][1])## we can add weight here 
            elif len(gram[0][0].split())==2:
                temp_co_big=temp_co_big+1
                if temp_co_big<4:
                    temp_list.append(gram[0][0])
                    temp_count_list.append(gram[0][1])
            else:
                temp_co_trig=temp_co_trig+1
                if temp_co_trig<4:
                    temp_list.append(gram[0][0])
                    temp_count_list.append(gram[0][1])
            
        tweets_list.append({"tweet":temp_text,"count":numpy.sum(temp_count_list),"grams":"|".join(temp_list)})
        
    
    tweets_data_Frame_list.append(DataFrame(tweets_list))
    

    print "Obtain the tweet sheet%d finished"%(i+1)
    # here we must clear up the redis databse in order to calculate for the next dataFrame
    r_server.flushdb()



############################################################################up
# remove replicating tweets
for k in range(1):
    tweets_data_Frame_list[k].sort(columns="count",ascending=0,inplace=True)
    tweets_data_Frame_list[k].reset_index(drop=True,inplace=True)
    
    tweets_data_Frame_list[k]["tag2"]=len(tweets_data_Frame_list[k])*[1]
    print len(tweets_data_Frame_list[k])
    for i in range(len(tweets_data_Frame_list[k])-1):
        temp_count_list=[]
        for j in range(i+1,len(tweets_data_Frame_list[k])):
            d=editdist.distance(tweets_data_Frame_list[k]["tweet"][i].encode('utf-8'),tweets_data_Frame_list[k]["tweet"][j].encode('utf-8'))
            
            if d==0:
                tweets_data_Frame_list[k]["tag2"][i]=0
                break
## remove spam tweets

for k in range(1):
        tweets_data_Frame_list[k]=tweets_data_Frame_list[k][tweets_data_Frame_list[k]['tag2']==1]
        tweets_data_Frame_list[k].reset_index(drop=True,inplace=True)
        tweet_real=[]
        print len(tweets_data_Frame_list[k])
        for i in range(len(tweets_data_Frame_list[k])):
            temp_count_list=[]
            baselen1=len(tweets_data_Frame_list[k]["tweet"][i].encode('utf-8'))
            
            for j in range(len(tweets_data_Frame_list[k])):
                baselen2=len(tweets_data_Frame_list[k]["tweet"][j].encode('utf-8'))
                d1=editdist.distance(tweets_data_Frame_list[k]["tweet"][i].encode('utf-8')[1:baselen1/2],tweets_data_Frame_list[k]["tweet"][j].encode('utf-8')[1:baselen2/2])
                d2=editdist.distance(tweets_data_Frame_list[k]["tweet"][i].encode('utf-8')[(baselen1/2+1):(baselen1-1)],tweets_data_Frame_list[k]["tweet"][j].encode('utf-8')[(baselen2/2+1):(baselen2-1)])
                temp_count_list.append((0.7*float(d1)+0.3*float(d2))/len(tweets_data_Frame_list[k]["tweet"][i].encode('utf-8'))*2)
                #print (0.7*float(d1)+0.3*float(d2))/len(tweets_data_Frame_list[k]["tweet"][i].encode('utf-8'))*2
            temp_count_list=[val for val in temp_count_list if val <0.6]
            if len(temp_count_list)>3:
                tweet_real.append(0)
            else:
                tweet_real.append(1)
        tweets_data_Frame_list[k]['tag1']=tweet_real
        #print "over=================================="
        #print "tag sheet%d finished"%(k+1)


# export the data into excel file
writer = ExcelWriter('temp1.xlsx')
for i in range(1):
    tweets_data_Frame_list[i][tweets_data_Frame_list[i]['tag1']==1].to_excel(writer,'sheet%d'%(i+1))
    writer.save()
    print "Exporting sheet%d finished!"%(i+1)

