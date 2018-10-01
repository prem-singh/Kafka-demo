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
public class AsynchronousProducer {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		String topicName = "Topic_Demo-1";
		String key ="keyword-1";
		String value ="Message sent by asynchronous producer!!.";
		
		Properties prop = new Properties();
		
		 prop.put("bootstrap.servers", "172.16.21.122:9093,172.16.21.122:9094");
		 prop.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
	     prop.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
	     
	     Producer<String, String> producer = new KafkaProducer <>(prop);
	     
	     ProducerRecord<String, String> record = new ProducerRecord<String, String>(topicName, key,value);
	     
	     producer.send(record, new MyProducerCallback());
	     System.out.println("AsynchronousProducer call completed");
	     producer.close();
	     

	}

}
