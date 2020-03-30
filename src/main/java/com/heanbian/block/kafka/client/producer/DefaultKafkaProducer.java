package com.heanbian.block.kafka.client.producer;

import static java.util.Objects.requireNonNull;
import static org.apache.kafka.clients.producer.ProducerConfig.ACKS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.BATCH_SIZE_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.BUFFER_MEMORY_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.COMPRESSION_TYPE_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.LINGER_MS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION;
import static org.apache.kafka.clients.producer.ProducerConfig.MAX_REQUEST_SIZE_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.RETRIES_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;

import java.util.Properties;
import java.util.UUID;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Value;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class DefaultKafkaProducer implements InitializingBean {

	private final ObjectMapper mapper = new ObjectMapper();

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
		requireNonNull(value, "value must not be null");
		try {
			byte[] buff = mapper.writeValueAsBytes(value);
			send(topic, key, buff, callback);
		} catch (JsonProcessingException e) {
			throw new RuntimeException(e);
		}
	}

	public void send(String topic, String key, byte[] value, Callback callback) {
		requireNonNull(topic, "topic must not be null");
		requireNonNull(value, "value must not be null");

		if (key == null) {
			key = UUID.randomUUID().toString();
		}
		if (callback == null) {
			callback = new DefaultKafkaCallback();
		}

		producer = new KafkaProducer<>(producerProperties());
		ProducerRecord<String, byte[]> record = new ProducerRecord<>(topic, key, value);
		producer.send(record, callback);
		producer.flush();
	}

	@Override
	public void afterPropertiesSet() throws Exception {
		requireNonNull(kafkaServers, "'kafka.servers' must be setting");
	}

	private Properties producerProperties() {
		Properties p = new Properties();
		p.put(BOOTSTRAP_SERVERS_CONFIG, kafkaServers);
		p.put(KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
		p.put(VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer");
		p.put(ACKS_CONFIG, "1");
		p.put(BUFFER_MEMORY_CONFIG, "33554432");
		p.put(COMPRESSION_TYPE_CONFIG, "none");
		p.put(RETRIES_CONFIG, "3");
		p.put(BATCH_SIZE_CONFIG, "16384");
		p.put(LINGER_MS_CONFIG, "0");
		p.put(MAX_REQUEST_SIZE_CONFIG, "104857600");
		p.put(REQUEST_TIMEOUT_MS_CONFIG, "30000");
		p.put(MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5");
		p.put(ENABLE_IDEMPOTENCE_CONFIG, "false");
		return p;
	}

}
