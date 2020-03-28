package com.heanbian.block.kafka.client.autoconfigure;

import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Role;

import com.heanbian.block.kafka.client.consumer.DefaultKafkaConsumer;
import com.heanbian.block.kafka.client.producer.DefaultKafkaProducer;

@Configuration
public class KafkaConfiguration {

	@Bean
	@Role(BeanDefinition.ROLE_INFRASTRUCTURE)
	public KafkaListenerBeanPostProcessor kafkaListenerBeanPostProcessor() {
		return new KafkaListenerBeanPostProcessor();
	}

	@Bean
	public DefaultKafkaConsumer defaultKafkaConsumer() {
		return new DefaultKafkaConsumer();
	}

	@Bean
	public DefaultKafkaProducer defaultKafkaProducer() {
		return new DefaultKafkaProducer();
	}
}