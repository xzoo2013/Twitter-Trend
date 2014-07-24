Twitter Trend Analysis
=============

This is a research project for the course T-61.5910 (Aalto University) and it is supervised by PhD Michael Mathioudakis.

Report abstract:

Many researches have been built up by analysing the textual contents in tweets. In this project, we are interested in their topics and try to find out the most popular ones, namely trends. Moreover, a series of algorithms are developed including stop word generation,
trends and spam tweet detection and representative tweet identification. In the end, these algorithms are applied on the real tweet data downloaded from Twitter API and a result evaluation will be provided. 

Main features from tech perspective:
1. Handling several millions of tweets, MongoDB supported
2. Redis is used to support the intermediate calculation due to its powerful data structure supporting. 
3. Scripts are mainly written in Python. Particularly, pandas , a python Data Analysis library is heavily used. 

Extra finding notes:

(Many thanks to Michael, I have learned a lot from the project. )

Natural language processing is never a trivial task. I think the most difficult part for this trend detection task is the identification of the spam tweets, such as the automatically generated tweets from various Apps. One feature of spam tweets is their unusual high frequency. One way to detect the spam tweets is based on the observation that spam tweets are usually sharing a uniform and stable format.

In this git folder, you can find the final project report and the most important sample codes. 

