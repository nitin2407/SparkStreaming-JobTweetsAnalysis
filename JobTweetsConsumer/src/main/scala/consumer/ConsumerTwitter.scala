package consumer;


import scala.collection.JavaConverters._
import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import org.joda.time.DateTime
import twitter4j.Status
import twitter4j._
import twitter4j.TwitterObjectFactory
import org.apache.spark.streaming.Seconds    

import com.mongodb.spark._
import com.mongodb.spark.config.WriteConfig
import org.apache.spark.api.java.{JavaRDD, JavaSparkContext}
import org.apache.spark.rdd.RDD
import com.mongodb.spark.config
import com.mongodb.spark.config._

import com.mongodb.spark.config.WriteConfig
import org.bson.Document

import twitter4j.conf.ConfigurationBuilder    
import twitter4j.auth.OAuthAuthorization
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream

import org.apache.kafka.common.serialization.StringDeserializer;
import java.util.concurrent._;
import scala.collection._;
import java.util.Properties;
import org.apache.spark._;
import org.apache.spark.streaming._;

import _root_.kafka.serializer.DefaultDecoder;
import _root_.kafka.serializer.StringDecoder;

import org.apache.spark.streaming.kafka010._;
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent;
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe;

import com.mongodb.spark.config.WriteConfig
import org.apache.spark.api.java.{JavaRDD, JavaSparkContext}
import org.apache.spark.rdd.RDD
import org.bson.conversions.Bson
import org.bson.{BsonDocument, Document}
import com.mongodb.spark.rdd.MongoRDD


import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import com.mongodb.spark.DefaultHelper
import com.mongodb.spark.rdd.MongoRDD
import com.mongodb.spark._
import com.mongodb.spark

import org.bson.conversions.Bson
import org.bson.{BsonDocument, Document}
import com.mongodb.client.MongoCollection
import com.mongodb.client.model.{InsertOneModel, ReplaceOneModel, UpdateOptions}
import com.mongodb.spark.DefaultHelper.DefaultsTo
import com.mongodb.spark.config.{ReadConfig, WriteConfig}
import com.mongodb.spark.rdd.MongoRDD
import com.mongodb.spark.rdd.api.java.JavaMongoRDD
import com.mongodb.spark.api.java.MongoSpark
import org.bson.BasicBSONObject

object ConsumerTwitter {
  def main(args: Array[String]) {
    println( "Kafka Consumer starting!" )
    //println("concat arguments = " )
    val config = new SparkConf().setAppName("twitter-job-stream").setMaster("local[*]").set("spark.driver.host", "localhost").set("spark.mongodb.input.uri","mongodb://127.0.0.1/test.myCollection?readPreference=primaryPreferred").set("spark.mongodb.output.uri", "mongodb://127.0.0.1/test.myCollection")
    val sc = new SparkContext(config)
    sc.setLogLevel("WARN")
    val ssc = new StreamingContext(sc, Seconds(5))
    
    val kafkaParams = Map[String, Object](
     // "zookeeper.connect" -> "localhost:2181",
      "bootstrap.servers" -> "localhost:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "group.id=test-consumer-group",
      "auto.offset.reset" -> "latest");
  
    val topic = Set("jobtweets");
    
    
   val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topic, kafkaParams));
   
   val rawTweets = stream.map(record=>(record.value().toString))
   
   val tweets = rawTweets.map(TwitterObjectFactory.createStatus)
    
    val entweets = tweets.filter(status => status.getLang == "en") 
    val data = entweets.map {status => 
                        val hashTagString = status.getText.split(" ").filter(_.startsWith("#")).mkString(";").replace("#","")  
                        
                        val tags = hashTagString.split(";").map(_.toLowerCase)
                          if(tags.contains("itjobs") || tags.contains("it")){
                           val jobtype = "IT job"   
                           (status.getText, status.getUser.getName,hashTagString,jobtype)
                          }
                          else if(tags.contains("freelance")){
                           val jobtype = "Freelance"
                           (status.getText, status.getUser.getName,hashTagString,jobtype)
                          }
                          else if(tags.contains("resume")){
                           val jobtype = "Resume"
                           (status.getText, status.getUser.getName,hashTagString,jobtype)
                          }
                          else{
                           val jobtype = "Other"
                           (status.getText, status.getUser.getName,hashTagString,jobtype)
                          }
                         
   }  
      val writeConfig = WriteConfig(Map("database"->"test","collection" -> "Jobs", "writeConcern.w" -> "majority"), Some(WriteConfig(sc)))
      data.foreachRDD{x=>
      x.map(mapToDocument).saveToMongoDB(writeConfig)
    }
      
    ssc.start();
    ssc.awaitTermination();
  }
  
  def mapToDocument(tuple : (String,String,String,String)):
  Document = {
    val doc = new Document()
    doc.put("status",tuple._1)
    doc.put("user",tuple._2)
    doc.put("hastags" , tuple._3)
    doc.put("JobType" , tuple._4)
    return doc
  }
}