package com.heanbian.block.kafka.client.config;

import static org.springframework.aop.support.AopUtils.getTargetClass;
import static org.springframework.core.annotation.AnnotatedElementUtils.findMergedAnnotation;

import java.lang.reflect.Method;
import java.util.Map;
import java.util.Map.Entry;

import org.springframework.beans.BeansException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.core.MethodIntrospector;
import org.springframework.core.MethodIntrospector.MetadataLookup;

import com.heanbian.block.kafka.client.annotation.KafkaListener;
import com.heanbian.block.kafka.client.consumer.DefaultKafkaConsumer;

public class KafkaListenerBeanPostProcessor implements BeanPostProcessor {

	@Autowired
	private DefaultKafkaConsumer defaultKafkaConsumer;

	@Override
	public Object postProcessAfterInitialization(final Object bean, final String beanName) throws BeansException {
		Class<?> clazz = getTargetClass(bean);
		Map<Method, KafkaListener> annotatedMethods = MethodIntrospector.selectMethods(clazz,
				(MetadataLookup<KafkaListener>) method -> findMergedAnnotation(method, KafkaListener.class));
		if (!annotatedMethods.isEmpty()) {
			for (Entry<Method, KafkaListener> entry : annotatedMethods.entrySet()) {
				defaultKafkaConsumer.consume(bean, entry.getKey(), entry.getValue());
			}
		}
		return bean;
	}

}