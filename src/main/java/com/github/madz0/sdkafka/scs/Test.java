package com.github.madz0.sdkafka.scs;

import static com.github.madz0.sdkafka.scs.SpringCloudStreamConfiguration.TOPIC;

import java.util.Properties;
import java.util.function.Consumer;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.cloud.stream.function.StreamBridge;
import org.springframework.context.ApplicationListener;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.messaging.Message;

@SpringBootApplication
public class Test {
  public static void main(String[] args) {
    SpringApplication.run(Test.class, args);
  }

  @Bean
  ApplicationListener<ApplicationReadyEvent> applicationEvent(StreamBridge streamBridge) {
    return e -> {
      streamBridge.send("scsPvTopic-out-0", new PV("dashboard", 15));
    };
  }
}

@Configuration(proxyBeanMethods = false)
@PropertySource("classpath:scs-config.properties")
class SpringCloudStreamConfiguration {
  static final String TOPIC = "scs-pv-topic";

  @Bean
  Consumer<Message<PV>> consumeFromScs() {
    return message -> System.out.printf("received %s%n", message.getPayload());
  }
}

record PV(String pageName, int count) {}
