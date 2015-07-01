
#[EAT SLEEP TWEET REPEAT](http://eatsleeptweetrepeat.itsbeta.com/index#home)
Author: Barsha Shrestha

1. Introduction
2. Data sources
3. Pipeline
  - Ingestion
  - File Distribution
  - Batch Processing
  - Serving Layer
  - Front End
  
#1 Introduction

Twitter plays a significant role in any setting these days. Whether it's breaking news or big events, Twitter is omnipresent. So I wanted to learn how tweets can affect the outcome of any situation. I chose to relate movie tweets to success of movies in the box office in particular. In the entertainment industry, it's said that no publicity is bad publicity. If such is the case, will people talking about Jurassic World, regardless of it being positive or negative, have any impact on how much money the movie makes?

#2 Data sources
I gathered my data from the streaming API of Twitter using the Tweepy library. From The Movie Database(TMDB) API, I was able to get the movie name, release date and the vote count for the movie, which I assumed to be a parameter to measure the success of the movie, because it was one of the few movie database APIs that got updated every day.

#3 Pipeline

![alt tag](https://raw.github.com/barshashrest/Insight-EatSleepTweetRepeat/Pipeline.png)

## Ingestion

The raw data that I got from Twitter and TMDB were both in JSON format. Messages were produced and published to Kafka topics, from where on the Kafka clusters served them to the consumers where the messages were consumed and written to HDFS.

##File Distribution
The Hadoop cluster receives the data from the consumers and breaks it into smaller pieces called blocks so as to store large data in a distributed fashion throughout the cluster. This partionining into blocks makes map and reduce jobs faster as they can now be applied to smaller subsets of large datasets. Finally the data is stored in block sizes of 90-95MB (out of 128MB).

##Batch Processing
I used Spark and SparkSQL to process the data in batches from HDFS and make my queries. For this part, I had to get all movies that are in theatres from TMDB then map the movie name to a tweet text which contained the movie name. I also had to map the day of movie data extraction to the TMDB table as it wasn't provided in their JSON file. After this I had two tables: one which showed me all the tweets that contained a movie name from the movie list and the movie name itself for that day and another table which gave me the movie name, release date and the vote count of the movie for a particular day. Therefore I had to join these tables based on both the movie name and the day of the data collection (both tweets and updated movie list with vote count). Finally I had a table that I could use in my serving layer.

##Serving Layer
I had to group all the tweets based on movie name and the date of the creation of the tweet because I wanted to check the impact of tweets on a day-to-day basis with the vote count received by the movie. For this, Cassandra was an ideal serving layer because I could save all the info based on movie name and have the day as the clustering column so that I could get each day's stats for any upcoming and now playing movies. Finally, the Cassandra table could be used to fetch data for my front-end.

##Front-end






