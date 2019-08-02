package com.heanbian.block.kafka.client.producer;

import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.UUID;

import javax.annotation.Resource;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Value;

import com.alibaba.fastjson.JSON;

public class DefaultKafkaProducer implements InitializingBean {
	private static final Logger LOGGER = LoggerFactory.getLogger(DefaultKafkaProducer.class);

	@Value("${kafka.servers:}")
	private String kafkaServers;

	@Resource(name = "com.heanbian.block.kafka.client.producer.ProducerProperties")
	private Properties producerProperties;

	private KafkaProducer<String, byte[]> producer;

	public <T> void send(String topic, List<T> datas) {
		if (datas != null && datas.size() > 0) {
			datas.forEach(d -> send(topic, d));
		}
	}

	public <T> void send(String topic, T data) {
		Objects.requireNonNull(topic, "topic must be set");
		Objects.requireNonNull(data, "data must not be null");
		send(topic, UUID.randomUUID().toString(), JSON.toJSONBytes(data));
	}

	private void send(String topic, String key, byte[] value) {
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

	@Override
	public void afterPropertiesSet() throws Exception {
		Objects.requireNonNull(kafkaServers, "kafka.servers must be set");
		producerProperties.put("bootstrap.servers", kafkaServers);
	}

}
