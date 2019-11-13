package com.heanbian.block.kafka.client.consumer;

import java.lang.reflect.Method;
import java.nio.charset.Charset;
import java.time.Duration;
import java.util.Arrays;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Async;

import com.alibaba.fastjson.JSON;
import com.heanbian.block.kafka.client.annotation.KafkaListener;

public class DefaultKafkaConsumer implements InitializingBean {
	private static final Logger LOGGER = LoggerFactory.getLogger(DefaultKafkaConsumer.class);

	@Value("${kafka.servers:}")
	private String kafkaServers;

	@Async
	public void consume(Object bean, Method method, KafkaListener kafkaListener) {
		final int poolSize = kafkaListener.threadCount();
		ExecutorService executor = new ThreadPoolExecutor(poolSize, poolSize, 0L, TimeUnit.MILLISECONDS, new SynchronousQueue<Runnable>(), new ThreadPoolExecutor.CallerRunsPolicy());

		KafkaConsumer<String, byte[]> kafkaConsumer = new KafkaConsumer<>(getConsumerProperties(kafkaListener.groupId()));
		kafkaConsumer.subscribe(Arrays.asList(kafkaListener.topic()));
		try {
			for (;;) {
				ConsumerRecords<String, byte[]> records = kafkaConsumer.poll(Duration.ofSeconds(1));
				if (!records.isEmpty()) {
					executor.submit(new ExecutorConsumer(bean, method, records));
				}
			}
		} finally {
			kafkaConsumer.close();
		}
	}

	public static class ExecutorConsumer implements Runnable {
		private Object bean;
		private Method method;
		private ConsumerRecords<String, byte[]> records;

		public ExecutorConsumer(Object bean, Method method, ConsumerRecords<String, byte[]> records) {
			this.bean = bean;
			this.method = method;
			this.records = records;
		}

		@Override
		public void run() {
			try {
				Class<?> valueClass = method.getParameterTypes()[0];
				method.setAccessible(true);

				for (ConsumerRecord<String, byte[]> record : records) {
					String metadata = new String(record.value(), Charset.defaultCharset());
					method.invoke(bean, (valueClass == String.class) ? metadata : JSON.parseObject(metadata, valueClass));
				}
			} catch (Exception e) {
				if (LOGGER.isErrorEnabled()) {
					LOGGER.error(e.getMessage(), e);
				}
			}
		}
	}

	@Override
	public void afterPropertiesSet() throws Exception {
		Objects.requireNonNull(kafkaServers, "'kafka.servers' must be setting");
	}
	
	private Properties getConsumerProperties(String groupId) {
		Properties consumerProperties = new Properties();
		consumerProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServers);
		consumerProperties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
		consumerProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
		consumerProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer");
		consumerProperties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
		consumerProperties.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
		return consumerProperties;
	}

}
