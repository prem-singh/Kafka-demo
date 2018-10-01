/**
 * 
 */
package com.tricon.test.Consumer;

import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import com.tricon.test.Producer.Supplier;

/**
 * @author premsingh
 *
 */
public class ManualConsumer {

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
        props.put("enable.auto.commit", "false");
        
        KafkaConsumer<String, Supplier> consumer = null;
        
        try {
            consumer = new KafkaConsumer<>(props);
            consumer.subscribe(Arrays.asList(topicName));

            while (true){
                ConsumerRecords<String, Supplier> records = consumer.poll(100);
                for (ConsumerRecord<String, Supplier> record : records){
                    System.out.println("Supplier id= " + String.valueOf(record.value().getID()) + " Supplier  Name = " + record.value().getName() + " Supplier Start Date = " + record.value().getStartDate().toString());
                }
                consumer.commitAsync();
            }
        }catch(Exception ex){
            ex.printStackTrace();
        }finally{
            consumer.commitSync();
            consumer.close();
        }


	}

}
