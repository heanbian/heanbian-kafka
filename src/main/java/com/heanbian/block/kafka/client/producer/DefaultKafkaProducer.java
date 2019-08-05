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

	public <T> void send(String topic, List<T> datas) {
		if (datas != null && datas.size() > 0) {
			datas.forEach(d -> send(topic, d));
		}
	}

	public <T> void send(String topic, T data) {
		Objects.requireNonNull(topic, "topic must be set");
		Objects.requireNonNull(data, "data must not be null");
		send(topic, UUID.randomUUID().toString(), JSON.toJSONBytes(data), new DefaultKafkaCallback());
	}

	public <T> void send(String topic, T data, Callback callback) {
		Objects.requireNonNull(topic, "topic must be set");
		Objects.requireNonNull(data, "data must not be null");
		send(topic, UUID.randomUUID().toString(), JSON.toJSONBytes(data), callback);
	}

	public void send(String topic, String key, byte[] value, Callback callback) {
		send(this.producerProperties, topic, key, value, callback);
	}

	public void send(Properties producerProperties, String topic, String key, byte[] value, Callback callback) {
		Objects.requireNonNull(producerProperties, "producerProperties must be set");
		Objects.requireNonNull(topic, "topic must not be null");
		Objects.requireNonNull(value, "value must not be null");

		key = Optional.ofNullable(key).orElse(UUID.randomUUID().toString());

		producer = new KafkaProducer<>(producerProperties);
		ProducerRecord<String, byte[]> record = new ProducerRecord<>(topic, key, value);
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
