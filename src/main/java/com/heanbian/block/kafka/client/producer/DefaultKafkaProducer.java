package com.heanbian.block.kafka.client.producer;

import java.util.List;
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

	private Properties producerProperties;
	private KafkaProducer<String, byte[]> producer;

	public <T> void send(String topic, List<T> messages) {
		if (messages != null && messages.size() > 0) {
			messages.forEach(d -> send(topic, d));
		}
	}

	public <T> void send(String topic, T message) {
		Objects.requireNonNull(topic, "topic must be set");
		Objects.requireNonNull(message, "message must not be null");
		send(topic, UUID.randomUUID().toString(), JSON.toJSONBytes(message), new DefaultKafkaCallback());
	}

	public <T> void send(String topic, T message, Callback callback) {
		Objects.requireNonNull(topic, "topic must be set");
		Objects.requireNonNull(message, "message must not be null");
		send(topic, UUID.randomUUID().toString(), JSON.toJSONBytes(message), callback);
	}

	public void send(String topic, String key, byte[] messageBytes, Callback callback) {
		send(this.producerProperties, topic, key, messageBytes, callback);
	}

	public void send(Properties producerProperties, String topic, String key, byte[] messageBytes, Callback callback) {
		Objects.requireNonNull(producerProperties, "producerProperties must be set");
		Objects.requireNonNull(topic, "topic must not be null");
		Objects.requireNonNull(messageBytes, "messageBytes must not be null");

		if (key == null) {
			key = UUID.randomUUID().toString();
		}

		producer = new KafkaProducer<>(producerProperties);
		ProducerRecord<String, byte[]> record = new ProducerRecord<>(topic, key, messageBytes);
		producer.send(record, callback);
		producer.flush();
	}

	@Override
	public void afterPropertiesSet() throws Exception {
		Objects.requireNonNull(kafkaServers, "kafka.servers must be set");
		producerProperties = new Properties();
		producerProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServers);
		producerProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
				"org.apache.kafka.common.serialization.StringSerializer");
		producerProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
				"org.apache.kafka.common.serialization.ByteArraySerializer");
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
	}

}
