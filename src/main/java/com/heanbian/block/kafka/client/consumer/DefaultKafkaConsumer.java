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

	private Properties consumerProperties;

	@Async
	public void consumeAsync(Object bean, Method method, KafkaListener kafkaListener) {
		String[] topics = kafkaListener.topics();

		if (topics == null || topics.length <= 0) {
			if (kafkaListener.topic().equals("")) {
				throw new RuntimeException("@KafkaListener topics must be set");
			}
			topics = new String[] { kafkaListener.topic() };
		}

		if (kafkaListener.groupId().equals("")) {
			throw new RuntimeException("@KafkaListener groupId must be set");
		}

		consumerProperties.put("group.id", kafkaListener.groupId());

		Class<?> valueClass = kafkaListener.valueClass();
		int thread = kafkaListener.thread() < 1 ? 1 : kafkaListener.thread();
		int count = kafkaListener.consumer() < 1 ? 1 : kafkaListener.consumer();
		while (--count >= 0) {
			consume(bean, method, topics, thread, valueClass);
		}
	}

	private void consume(Object bean, Method method, String[] topics, int thread, Class<?> valueClass) {
		ExecutorService executor = new ThreadPoolExecutor(thread, thread, 0L, TimeUnit.MILLISECONDS,
				new SynchronousQueue<Runnable>(), new ThreadPoolExecutor.CallerRunsPolicy());

		KafkaConsumer<String, byte[]> kafkaConsumer = new KafkaConsumer<>(consumerProperties);
		kafkaConsumer.subscribe(Arrays.asList(topics));
		try {
			for (;;) {
				ConsumerRecords<String, byte[]> records = kafkaConsumer.poll(Duration.ofSeconds(1));
				if (!records.isEmpty()) {
					executor.submit(new ExecutorConsumer(bean, method, records, valueClass));
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
		private Class<?> valueClass;

		public ExecutorConsumer(Object bean, Method method, ConsumerRecords<String, byte[]> records,
				Class<?> valueClass) {
			this.bean = bean;
			this.method = method;
			this.records = records;
			this.valueClass = valueClass;
		}

		@Override
		public void run() {
			try {
				for (ConsumerRecord<String, byte[]> record : records) {
					method.setAccessible(true);
					method.invoke(bean, (valueClass == String.class)
							? new String(record.value(), Charset.defaultCharset())
							: JSON.parseObject(new String(record.value(), Charset.defaultCharset()), valueClass));
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
		Objects.requireNonNull(kafkaServers, "kafka.servers must be set");
		consumerProperties = new Properties();
		consumerProperties.put("bootstrap.servers", kafkaServers);
		consumerProperties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		consumerProperties.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
		consumerProperties.put("enable.auto.commit", "true");
		consumerProperties.put("auto.commit.interval.ms", "1000");
	}

}
