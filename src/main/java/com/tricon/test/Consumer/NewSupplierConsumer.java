/**
 * 
 */
package com.tricon.test.Consumer;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import com.tricon.test.Producer.Supplier;

/**
 * @author premsingh
 * Reading properties from properties file
 */
public class NewSupplierConsumer {

	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
		String topicName = "SupplierTopic-Demo";
      //  String groupName = "SupplierTopicGroup";
        Properties props = new Properties();
        
        InputStream input = null;
        KafkaConsumer<String, Supplier> consumer = null;
        
        try {
            input = new FileInputStream("SupplierConsumer.properties");
            props.load(input);
            consumer = new KafkaConsumer<>(props);
            consumer.subscribe(Arrays.asList(topicName));

            while (true){
                ConsumerRecords<String, Supplier> records = consumer.poll(100);
                for (ConsumerRecord<String, Supplier> record : records){
                    System.out.println("Supplier id= " + String.valueOf(record.value().getID()) + " Supplier  Name = " + record.value().getName() + " Supplier Start Date = " + record.value().getStartDate().toString());
                }
            }
        }catch(Exception ex){
            ex.printStackTrace();
        }finally{
            input.close();
            consumer.close();
        }
	}

}
