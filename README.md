# heanbian-kafka-client

### 使用方法

1.Spring boot 2.x 项目启动类 XxxApplication 上注解 `@EnableKafkaClient`

样例:
```java

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import com.heanbian.block.kafka.client.annotation.EnableKafkaClient;

@EnableKafkaClient
@SpringBootApplication
public class App {

	public static void main(String[] args) {
		SpringApplication.run(App.class, args);
	}

}

```

2.定义任意 @Component 类，并在其方法是加注解 `@KafkaListener`

样例：

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
