/**
 * 
 */
package com.tricon.test.Producer;

import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

/**
 * @author premsingh
 *
 */
public class SupplierConsumer {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		
		String topicName = "SupplierTopic-Demo";
        String groupName = "SupplierTopicGroup";
        
        Properties props = new Properties();
        props.put("bootstrap.servers", "172.16.21.122:9093,172.16.21.122:9094");
        props.put("group.id", groupName);
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer", "com.tricon.test.Producer.SupplierDeserializer");
		
		KafkaConsumer<String, Supplier> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList(topicName));
        while (true){
            ConsumerRecords<String, Supplier> records = consumer.poll(100);
            for (ConsumerRecord<String, Supplier> record : records){
                    System.out.println("Supplier id= " + String.valueOf(record.value().getID()) + " Supplier  Name = " + record.value().getName() + " Supplier Start Date = " + record.value().getStartDate().toString());
            }
    }
        


	}

}
