package com.heanbian.block.kafka.client.producer;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DefaultKafkaCallback implements Callback {
	private static final Logger LOGGER = LoggerFactory.getLogger(DefaultKafkaCallback.class);

	@Override
	public void onCompletion(RecordMetadata metadata, Exception exception) {
		if (exception != null && LOGGER.isErrorEnabled()) {
			LOGGER.error(exception.getMessage(), exception);
		}
	}

}
