package com.tricon.test.Producer;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

public class SynchronousProducer {

	public static void main(String[] args) {
		String topicName = "Topic_Demo-1";
		String key ="keyword-1";
		String value ="Tricon Infotech Pvt. Ltd.";
		
		Properties prop = new Properties();
		
		 prop.put("bootstrap.servers", "172.16.21.122:9093,172.16.21.122:9094");
		 prop.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
	     prop.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
	     
	     Producer<String, String> producer = new KafkaProducer <>(prop);
	     
	     ProducerRecord<String, String> record = new ProducerRecord<String, String>(topicName, key,value);
	     
	     try{
	           RecordMetadata metadata = producer.send(record).get();
	           System.out.println("Message is sent to Partition no " + metadata.partition() + " and offset " + metadata.offset());
	           System.out.println("SynchronousProducer Completed with success.");
	      }catch (Exception e) {
	           e.printStackTrace();
	           System.out.println("SynchronousProducer failed with an exception");
	      }finally{
	           producer.close();
	      }

	}

}
