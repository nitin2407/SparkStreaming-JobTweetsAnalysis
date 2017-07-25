package producer;


import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import twitter4j.FilterQuery;
import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.TwitterObjectFactory;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.conf.ConfigurationBuilder;

public class KafkaTweetsProducer {
    
	private static final Logger logger = LoggerFactory.getLogger(KafkaTweetsProducer.class);
    
    /** Information necessary for accessing the Twitter API */
    private String consumerKey;
    private String consumerSecret;
    private String accessToken;
    private String accessTokenSecret;

    private TwitterStream twitterStream;
    
    private void start() {
	
		Properties configProperties = new Properties();
		configProperties = new Properties();
		configProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		configProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
			"org.apache.kafka.common.serialization.ByteArraySerializer");
		configProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
			"org.apache.kafka.common.serialization.StringSerializer");

		final Producer producer = new KafkaProducer(configProperties);
	
		/** Twitter properties **/
		consumerKey = "key";
		consumerSecret = "secret";
		accessToken = "token";
		accessTokenSecret = "tokensecret";
		
		ConfigurationBuilder cb = new ConfigurationBuilder();
		cb.setOAuthConsumerKey(consumerKey);
		cb.setOAuthConsumerSecret(consumerSecret);
		cb.setOAuthAccessToken(accessToken);
		cb.setOAuthAccessTokenSecret(accessTokenSecret);
		cb.setJSONStoreEnabled(true);
		cb.setIncludeEntitiesEnabled(true);
		
		twitterStream = new TwitterStreamFactory(cb.build()).getInstance();
		final Map<String, String> headers = new HashMap<String, String>();
		
		/** Twitter listener **/
		StatusListener listener = new StatusListener() {
			// The onStatus method is executed every time a new tweet comes in
			public void onStatus(Status status) {
				ProducerRecord<String, String> data = new ProducerRecord<String, String>("jobtweets"
														 , TwitterObjectFactory.getRawJSON(status));
						
				
				producer.send(data);
				
			}
		    
			public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {}
			
			public void onTrackLimitationNotice(int numberOfLimitedStatuses) {}
			
			public void onScrubGeo(long userId, long upToStatusId) {}
			
			public void onException(Exception ex) {
				logger.info("Shutting down Twitter job stream...");
				twitterStream.shutdown();
			}
			
			public void onStallWarning(StallWarning warning) {}
	    };
	
		/** Bind the listener **/
		twitterStream.addListener(listener);
		FilterQuery fq = new FilterQuery();
		//fq.count(0);
		String[] keywordsArray = { "itjobs", "freelance","resume","jobs" };
		fq.track(keywordsArray);
		twitterStream.filter(fq);  
	}
    
	public static void main(String[] args) {
		try {
			System.out.println("Kafka producer starting..!");
			KafkaTweetsProducer producer = new KafkaTweetsProducer();
				
			producer.start();
				
		} catch (Exception e) {
			logger.info(e.getMessage());
		}
		
	}
}
