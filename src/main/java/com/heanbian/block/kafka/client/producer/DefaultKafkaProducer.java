package com.heanbian.block.kafka.client.producer;

import java.util.Objects;
import java.util.Properties;
import java.util.UUID;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Value;

import com.alibaba.fastjson.JSON;

public class DefaultKafkaProducer implements InitializingBean {

	@Value("${kafka.servers:}")
	private String kafkaServers;

	private KafkaProducer<String, byte[]> producer;

	public <T> void send(String topic, T value) {
		send(topic, value, null);
	}

	public <T> void send(String topic, T value, Callback callback) {
		send(topic, null, value, callback);
	}

	public <T> void send(String topic, String key, T value, Callback callback) {
		send(topic, key, JSON.toJSONBytes(value), callback);
	}

	public void send(String topic, String key, byte[] value, Callback callback) {
		Objects.requireNonNull(topic, "topic must not be null");
		Objects.requireNonNull(value, "value must not be null");

		if (key == null) {
			key = UUID.randomUUID().toString();
		}
		if (callback == null) {
			callback = new DefaultKafkaCallback();
		}

		producer = new KafkaProducer<>(getProducerProperties());
		ProducerRecord<String, byte[]> record = new ProducerRecord<>(topic, key, value);
		producer.send(record, callback);
		producer.flush();
	}

	@Override
	public void afterPropertiesSet() throws Exception {
		Objects.requireNonNull(kafkaServers, "'kafka.servers' must be setting");
	}
	
	private Properties getProducerProperties() {
		Properties producerProperties = new Properties();
		producerProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServers);
		producerProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
		producerProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer");
		producerProperties.put(ProducerConfig.ACKS_CONFIG, "1");
		producerProperties.put(ProducerConfig.BUFFER_MEMORY_CONFIG, "33554432");
		producerProperties.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "none");
		producerProperties.put(ProducerConfig.RETRIES_CONFIG, "3");
		producerProperties.put(ProducerConfig.BATCH_SIZE_CONFIG, "16384");
		producerProperties.put(ProducerConfig.LINGER_MS_CONFIG, "0");
		producerProperties.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, "104857600");
		producerProperties.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, "30000");
		producerProperties.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5");
		producerProperties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "false");
		return producerProperties;
	}

}
