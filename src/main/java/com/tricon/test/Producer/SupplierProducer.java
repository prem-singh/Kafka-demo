/**
 * 
 */
package com.tricon.test.Producer;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

/**
 * @author premsingh
 *
 */
public class SupplierProducer {

	/**
	 * @param args
	 * @throws ExecutionException 
	 * @throws InterruptedException 
	 * @throws ParseException 
	 */
	public static void main(String[] args) throws InterruptedException, ExecutionException, ParseException {
		String topicName = "SupplierTopic-Demo";

		Properties props = new Properties();
		 props.put("bootstrap.servers", "172.16.21.122:9093,172.16.21.122:9094");
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "com.tricon.test.Producer.SupplierSerializer");

		Producer<String, Supplier> producer = new KafkaProducer<>(props);

		DateFormat df = new SimpleDateFormat("yyyy-MM-dd");
		Supplier sp1 = new Supplier(105, "Xyz Pvt Ltd.", df.parse("2016-04-01"));
		Supplier sp2 = new Supplier(106, "Abc Pvt Ltd.", df.parse("2012-01-01"));

		producer.send(new ProducerRecord<String, Supplier>(topicName, "SUP", sp1)).get();
		producer.send(new ProducerRecord<String, Supplier>(topicName, "SUP", sp2)).get();

		System.out.println("SupplierProducer Completed.");
		producer.close();

	}

}
