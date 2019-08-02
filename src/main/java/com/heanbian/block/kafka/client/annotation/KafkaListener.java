package com.heanbian.block.kafka.client.annotation;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import org.springframework.core.annotation.AliasFor;

@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface KafkaListener {

	@AliasFor("valueClass")
	Class<?> valueType() default String.class;

	@AliasFor("valueType")
	Class<?> valueClass() default String.class;

	@AliasFor("value")
	String topic() default "kafka-topic";

	@AliasFor("topic")
	String value() default "kafka-topic";

	@AliasFor("values")
	String[] topics() default {};

	@AliasFor("topics")
	String[] values() default {};

	@AliasFor("groupId")
	String id() default "kafka-group-id";

	@AliasFor("id")
	String groupId() default "kafka-group-id";

	int consumer() default 1;

	int thread() default 1;

}
