/**
 * 
 */
package com.tricon.test.Producer;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

/**
 * @author premsingh
 *
 */
public class SimpleProducer {
	
	

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		
		  String topicName = "Topic_demo";
		  String key = "keyword";
		  String value = "value for keyword";
		  
		  Properties props = new Properties();
		  props.put("bootstrap.servers", "172.16.21.122:9093,172.16.21.122:9094");
	      props.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");         
	      props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
	      
	      Producer<String, String> producer = new KafkaProducer <>(props);
	      
	      ProducerRecord<String, String> record = new ProducerRecord<>(topicName,key,value);
	      
	      producer.send(record);
	      
	      producer.close();
	      
	      System.out.println("Simple Producer completed!!");

	}

}
