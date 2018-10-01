/**
 * 
 */
package com.tricon.test.Consumer;

import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

/**
 * @author premsingh
 *
 */
public class RandomConsumer {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		
		String topicName = "RandomProducerTopic";
        KafkaConsumer<String, String> consumer = null;
        
        String groupName = "RG";
        Properties props = new Properties();
        props.put("bootstrap.servers", "172.16.21.122:9093,172.16.21.122:9094");
        props.put("group.id", groupName);
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("enable.auto.commit", "false");
        
        consumer = new KafkaConsumer<>(props);
        RebalanceListner rebalanceListner = new RebalanceListner(consumer);
        
        consumer.subscribe(Arrays.asList(topicName),rebalanceListner);
        try{
            while (true){
                ConsumerRecords<String, String> records = consumer.poll(100);
                for (ConsumerRecord<String, String> record : records){
                    //System.out.println("Topic:"+ record.topic() +" Partition:" + record.partition() + " Offset:" + record.offset() + " Value:"+ record.value());
                   // Do some processing and save it to Database
                    rebalanceListner.addOffset(record.topic(), record.partition(),record.offset());
                }
                    //consumer.commitSync(rebalanceListner.getCurrentOffsets());
            }
        }catch(Exception ex){
            System.out.println("Exception.");
            ex.printStackTrace();
        }
        finally{
                consumer.close();
        }
	}

}
