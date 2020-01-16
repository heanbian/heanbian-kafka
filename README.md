# heanbian-kafka-client

### `pom.xml` 添加，如下：

```xml

<dependency>
	<groupId>com.heanbian</groupId>
	<artifactId>heanbian-kafka-client</artifactId>
	<version>11.0.4</version>
</dependency>

```
注：JDK 11+ ，最新版本，可以到maven官网查找。

### `application.yml` 配置，样例：

```yaml

kafka.servers: IP1:port,IP2:port ...

```

### Spring boot 2.x 项目启动类 XxxApplication 上注解 `@EnableKafkaClient` 样例:

```java

import com.heanbian.block.kafka.client.annotation.EnableKafkaClient;

@EnableKafkaClient
@SpringBootApplication
public class XxxApplication {

	public static void main(String[] args) {
		SpringApplication.run(XxxApplication.class, args);
	}
}

```

### 定义任意 `@Component` 类，并在其方法上加注解 `@KafkaListener` 样例：

```java

import org.springframework.stereotype.Component;
import com.heanbian.block.kafka.client.annotation.KafkaListener;

@Component
public class XxxKafkaConsumer {

	@KafkaListener(topic =  "test")
	public void test(String var0) {
		// TODO
	}
}

```
