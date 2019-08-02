package com.heanbian.block.kafka.client.annotation;

import java.util.Map;
import java.util.Properties;

import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Role;
import org.yaml.snakeyaml.Yaml;

import com.heanbian.block.kafka.client.consumer.DefaultKafkaConsumer;
import com.heanbian.block.kafka.client.producer.DefaultKafkaProducer;

@Configuration
public class KafkaConfiguration {

	@SuppressWarnings("unchecked")
	@Bean("producerProperties")
	public Properties producerProperties() {
		Map<String, Object> producerMap = (Map<String, Object>) loadKafkaConfig().get("producer");
		Properties producer = new Properties();
		producerMap.forEach((key, value) -> {
			producer.put(key, value.toString());
		});
		return producer;
	}

	@SuppressWarnings("unchecked")
	@Bean("consumerProperties")
	public Properties consumerProperties() {
		Map<String, Object> consumerMap = (Map<String, Object>) loadKafkaConfig().get("consumer");
		Properties consumer = new Properties();
		consumerMap.forEach((key, value) -> {
			consumer.put(key, value.toString());
		});
		return consumer;
	}

	@SuppressWarnings("unchecked")
	private Map<String, Object> loadKafkaConfig() {
		Map<String, Object> es_config = new Yaml()
				.load(this.getClass().getClassLoader().getResourceAsStream("kafka-config.yml"));
		return (Map<String, Object>) es_config.get("kafka");
	}

	@Bean("com.heanbian.block.kafka.client.annotation.KafkaListenerBeanPostProcessor")
	@Role(BeanDefinition.ROLE_INFRASTRUCTURE)
	public KafkaListenerBeanPostProcessor kafkaListenerBeanPostProcessor() {
		return new KafkaListenerBeanPostProcessor();
	}

	@Bean("com.heanbian.block.kafka.client.consumer.DefaultKafkaConsumer")
	public DefaultKafkaConsumer defaultKafkaConsumer() {
		return new DefaultKafkaConsumer();
	}

	@Bean("com.heanbian.block.kafka.client.producer.DefaultKafkaProducer")
	public DefaultKafkaProducer defaultKafkaProducer() {
		return new DefaultKafkaProducer();
	}
}