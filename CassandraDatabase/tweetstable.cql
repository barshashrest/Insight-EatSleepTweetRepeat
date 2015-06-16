CREATE TABLE retweets( moviename text, userid int, userURL text, RTURL1 text, RTURL2 text, RTURL2id int, PRIMARY KEY (moviename, userid)) 
WITH CLUSTERIN ORDER BY (userid DESC);

