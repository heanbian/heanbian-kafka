[TOC]
# heanbian-kafka-client

### `pom.xml`添加，如下：

```xml
<dependency>
	<groupId>com.heanbian</groupId>
	<artifactId>heanbian-kafka-client</artifactId>
	<version>11.0.1</version>
</dependency>
```
注：具体最版本，可以到maven官网查找。

### Spring boot 2.x 项目启动类 XxxApplication 上注解 `@EnableKafkaClient` 样例:

```java

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import com.heanbian.block.kafka.client.annotation.EnableKafkaClient;

@EnableKafkaClient
@SpringBootApplication
public class XxxApplication {

	public static void main(String[] args) {
		SpringApplication.run(App.class, args);
	}

}

```

### 定义任意 `@Component` 类，并在其方法是加注解 `@KafkaListener` 样例：

```java

import org.springframework.stereotype.Component;
import com.heanbian.block.kafka.client.annotation.KafkaListener;

@Component
public class XxxKafkaConsumer {

	@KafkaListener(topics = { "test" })
	public void test(String var0) {
		// TODO
	}
}

```
### `application.yml` 配置，样例：

```yaml
kafka.servers: IP1:port,IP2:port ...
```
