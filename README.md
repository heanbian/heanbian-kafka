# heanbian-kafka-client

## 前提条件

JDK11+

## pom.xml

具体版本，可以上Maven中央仓库查询

```
<dependency>
	<groupId>com.heanbian</groupId>
	<artifactId>heanbian-kafka-client</artifactId>
	<version>11.3.0</version>
</dependency>
```

## 使用示例

配置 `application.yml`

```
kafka.servers:: 192.168.1.101:2181,192.168.1.102:2181,192.168.1.103:2181
```

Spring Boot 启动类添加注解：

`@EnableKafkaClient`

Java代码片段：

```
//生产者
@Autowired
private DefaultKafkaProducer defaultKafkaProducer;

```

```
//消费者
@Component
public class TestConsumer {

	@kafkaListener(topic = "test")
	public void consumer(String data) {
		// TODO

	}
	
	@kafkaListener(topic = "test-1")
	public void consumer(Test test) {
		// TODO

	}

}
```

说明：适用于 Spring Boot 2.x 项目。