package com.heanbian.block.kafka.client.producer;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.UUID;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
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

		key = Optional.ofNullable(key).orElse(UUID.randomUUID().toString());

		producer = new KafkaProducer<>(producerProperties);
		ProducerRecord<String, byte[]> record = new ProducerRecord<>(topic, key, messageBytes);
		producer.send(record, callback);
		producer.flush();
	}

	@Override
	public void afterPropertiesSet() throws Exception {
		Objects.requireNonNull(kafkaServers, "kafka.servers must be set");
		producerProperties = new Properties();
		producerProperties.put("bootstrap.servers", kafkaServers);
		producerProperties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		producerProperties.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
		producerProperties.put("acks", "1");
		producerProperties.put("buffer.memory", "33554432");
		producerProperties.put("compression.type", "none");
		producerProperties.put("retries", "3");
		producerProperties.put("batch.size", "16384");
		producerProperties.put("linger.ms", "0");
		producerProperties.put("max.request.size", "104857600");
		producerProperties.put("request.timeout.ms", "30000");
		producerProperties.put("max.in.flight.requests.per.connection", "5");
		producerProperties.put("enable.idempotence", "false");
	}

}
