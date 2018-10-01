package com.tricon.test.Producer;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

/**
 * Created topic with partition 10 manually. Need to check how to configure partition from .
 * @author premsingh
 *
 */
public class SensorProducer {

	public static void main(String[] args) {
		String topicName = "SensorTopic-5";

	      Properties props = new Properties();
	      props.put("bootstrap.servers", "172.16.21.122:9093,172.16.21.122:9094");
	      props.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
	      props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
	      props.put("partitioner.class", "com.tricon.test.Producer.SensorPartitioner");
	      props.put("speed.sensor.name", "TSS");
	    //  props.put("num.partitions", 10);
	      
	      Producer<String, String> producer = new KafkaProducer <>(props);
	      
	      for (int i=0 ; i<10 ; i++)
	          producer.send(new ProducerRecord<>(topicName,"SSP"+i,"500"+i));
	      
	      for (int i=0 ; i<10 ; i++)
	          producer.send(new ProducerRecord<>(topicName,"TSS","500"+i));
	      
	      producer.close();
	      System.out.println("SensorProducer Completed.");

	}

}
