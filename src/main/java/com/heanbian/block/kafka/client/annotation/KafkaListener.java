package com.heanbian.block.kafka.client.annotation;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface KafkaListener {

	String[] topics();

	String groupId() default "heanbian_kafka_groupId";

	int threadCount() default 1;

}
