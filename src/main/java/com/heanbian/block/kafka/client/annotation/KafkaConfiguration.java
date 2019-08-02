package com.heanbian.block.kafka.client.annotation;

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

	@Bean("com.heanbian.block.kafka.client.producer.ProducerProperties")
	public Properties producerProperties() {
		return (Properties) readKafkaConfig().get("producer");
	}

	@Bean("com.heanbian.block.kafka.client.consumer.ConsumerProperties")
	public Properties consumerProperties() {
		return (Properties) readKafkaConfig().get("consumer");
	}

	private Properties readKafkaConfig() {
		Yaml y = new Yaml();
		Properties p = y.loadAs(getClass().getClassLoader().getResourceAsStream("kafka-config.yml"), Properties.class);
		return (Properties) p.get("kafka");
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