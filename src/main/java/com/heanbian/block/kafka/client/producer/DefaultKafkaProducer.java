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
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.alibaba.fastjson.JSON;

@Component
public class DefaultKafkaProducer implements InitializingBean {
	private static final Logger LOGGER = LoggerFactory.getLogger(DefaultKafkaProducer.class);

	@Value("${kafka.servers:}")
	private String kafkaServers;

	@Resource(name = "producerProperties")
	private Properties producerProperties;

	private KafkaProducer<String, byte[]> producer;

	public <T> void send(String topic, T data) {
		send(topic, JSON.toJSONString(data));
	}

	private void send(String topic, String data) {
		Objects.requireNonNull(topic, "topic must be set");
		Objects.requireNonNull(data, "data must not be null");

		producer = new KafkaProducer<>(producerProperties);
		ProducerRecord<String, byte[]> record = new ProducerRecord<>(topic, data.getBytes());
		producer.send(record, new Callback() {

			@Override
			public void onCompletion(RecordMetadata metadata, Exception exception) {
				if (LOGGER.isErrorEnabled()) {
					LOGGER.error(exception.getMessage(), exception);
				}
			}
		});
		producer.flush();
	}

	@Override
	public void afterPropertiesSet() throws Exception {
		producerProperties.put("bootstrap.servers", kafkaServers);
	}

}
