# Insight-EatSleepTweetRepeat
#Author: Barsha Shrestha

EAT SLEEP TWEET REPEAT
http://eatsleeptweetrepeat.itsbeta.com/index#home

1. Introduction
2. Data sources
3. Pipeline
  - Ingestion
  - File Distribution
  - Batch Processing
  - Serving Layer
  - Front End
  

1. Introduction
Twitter plays a significant role in any setting these days. Whether it's breaking news or big events, Twitter is omnipresent. So I wanted to learn how tweets can affect the outcome of any situation. I chose to relate movie tweets to success of movies in the box office in particular. In the entertainment industry, it's said that no publicity is bad publicity. If such is the case, will people talking about Jurassic World, regardless of it being positive or negative, have any impact on how much money the movie makes?

2. Data sources
I gathered my data from the streaming API of Twitter using the Tweepy library. From The Movie Database(TMDB) API, I was able to get the movie name, release date and the vote count for the movie, which I assumed to be a parameter for the success of the movie, because it was one of the few movie database APIs that got updated everyday. 




