# SparkStreaming-JobTweetsAnalysis

## Description:

There are many job postings put up on twitter. This app is used for real-time processing of these tweets. Filtering, extracting and storing job information related to specific requirements. 

## Features:

This app consolidates jobs related to specific industries and makes it easier for a user looking for a job in his/her domain. The job data is formatted into tables with columns for details regarding the job, such as, the date, company, technology , link, etc. This also has the type of job, which specifies the industry it caters to.

## Spefications:

This app uses Twitter 4j library to get a stream of tweets. To get this stream, the app should be first authenticated, which is done using the credentials(app secret and app key), given by twitter after the app is registered on twitter developers portal. 

One major bottleneck occurs due to a rate at which the stream is received from twitter. Due to delay in processing engine, many tweets were getting lost. This was overcome by using Kakfa. Kafka works on pub-sub model and it acts a queuing system. It prevents the loss of tweets by buffering the tweets which are not processed. 

So, Kakfa producer gets the stream from Twitter 4j and creates its own stream and sends this stream to the Kafka Consumer. Kafka Consumer then, processes these tweets and converts them into required format. The job data is then stored into MongoDb Database, where each job has its own type, specifying the industry for that job.

Kafka Producer is written in Java and Consumer is written in Scala. The streaming engine for job processing is SparkStreaming.

## Configuration:

The project was configured using Maven. The important libraries used are Twitter4j, Spark Core and Kafka Core. Linux Operating System is used for this application.


