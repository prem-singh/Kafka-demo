package com.tricon.test.Producer;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;

public class MyProducerCallback implements Callback {

	@Override
	public void onCompletion(RecordMetadata metadata, Exception exception) {
		if (exception != null)
			System.out.println("AsynchronousProducer failed with an exception");
		else
			System.out.println("AsynchronousProducer call Success:");
	}

}
