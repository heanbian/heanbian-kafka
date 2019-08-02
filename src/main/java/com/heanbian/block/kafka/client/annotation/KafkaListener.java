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

	Class<?> clazz() default String.class;

	String[] topics() default {};

	String groupId() default "";

	int consumer() default 1;

	int thread() default 1;

}
