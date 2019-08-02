package com.heanbian.block.kafka.client.producer;

import java.util.Objects;
import java.util.Properties;

import javax.annotation.Resource;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;

import com.alibaba.fastjson.JSON;

public class DefaultKafkaProducer {
	private static final Logger LOGGER = LoggerFactory.getLogger(DefaultKafkaProducer.class);

	@Value("${kafka.servers:}")
	private String kafkaServers;

	@Resource(name = "producerProperties")
	private Properties producerProperties;

	private KafkaProducer<String, byte[]> producer;

	public <T> void send(String topic, T data) {
		send(topic, JSON.toJSONString(data), JSON.toJSONBytes(data));
	}

	private void send(String topic, String key, byte[] value) {
		Objects.requireNonNull(topic, "topic must be set");
		Objects.requireNonNull(key, "key must not be null");

		producerProperties.put("bootstrap.servers", kafkaServers);
		producer = new KafkaProducer<>(producerProperties);
		ProducerRecord<String, byte[]> record = new ProducerRecord<>(topic, key, value);
		producer.send(record, new Callback() {

			@Override
			public void onCompletion(RecordMetadata metadata, Exception exception) {
				if (exception != null && LOGGER.isErrorEnabled()) {
					LOGGER.error(exception.getMessage(), exception);
				}
			}
		});
		producer.flush();
	}

}
