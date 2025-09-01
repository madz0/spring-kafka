package com.github.madz0.sdkafka.purekafka;

import java.util.Map;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.ApplicationListener;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.converter.JsonMessageConverter;
import org.springframework.kafka.support.converter.MessageConverter;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.messaging.Message;

@SpringBootApplication
public class Test {
  public static void main(String[] args) {
    SpringApplication.run(Test.class, args);
  }

  @Bean
  ApplicationListener<ApplicationReadyEvent> applicationEvent(MyKafkaTemplate kafkaTemplate) {
    return e -> {
      kafkaTemplate.send(PureKafkaConfig.TOPIC, new PV("dashboard", 5));
    };
  }
}

@Configuration
class PureKafkaConfig {

  public static final String TOPIC = "topic";
  public static final String GROUP = "group";

  @Bean
  MessageConverter messageConverter() {
    return new JsonMessageConverter();
  }

  @Bean
  NewTopic newTopic() {
    return new NewTopic(TOPIC, (short) 1, (short) 1);
  }

  @Bean
  MyKafkaTemplate kafkaTemplate(ProducerFactory<Object, Object> producerFactory) {
    return new MyKafkaTemplate(
        producerFactory,
        Map.of(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class));
  }

  @KafkaListener(topics = TOPIC, groupId = GROUP)
  public void kafListener(Message<PV> message) {
    System.out.printf("received %s%n", message.getPayload());
  }
}

class MyKafkaTemplate extends KafkaTemplate<Object, Object> {

  public MyKafkaTemplate(
      ProducerFactory<Object, Object> producerFactory, Map<String, Object> configs) {
    super(producerFactory, configs);
  }
}

record PV(String pageName, int count) {}
