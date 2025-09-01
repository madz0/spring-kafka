package com.github.madz0.sdkafka.integration;

import java.util.Map;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.ApplicationListener;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.integration.annotation.ServiceActivator;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.dsl.MessageChannels;
import org.springframework.integration.kafka.dsl.Kafka;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.ConsumerProperties;
import org.springframework.kafka.support.converter.JsonMessageConverter;
import org.springframework.kafka.support.converter.MessageConverter;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.support.MessageBuilder;

@SpringBootApplication
public class Test {
  public static void main(String[] args) {
    SpringApplication.run(Test.class, args);
  }

  @Bean
  ApplicationListener<ApplicationReadyEvent> applicationEvent(MessageChannel outboundChannel) {
    return e -> {
      outboundChannel.send(MessageBuilder.withPayload(new PV("screen", 6)).build());
    };
  }
}

@Configuration
class IntegrationConfiguration {

  public static final String INTEGRATION_TOPIC = "integration_topic";

  /*
  A direct channel is a subscription channel. It doesn't have a
  receive() method. The only way with spring integration is this
  If we had defined for example QueueChannel, we could call it's blocking
  receive method.
   */
  @ServiceActivator(inputChannel = "inboundChannel")
  void consume(Message<PV> message) {
    System.out.printf("received %s%n", message.getPayload());
  }

  @Bean
  IntegrationFlow inbound(
      MessageChannel inboundChannel, ConsumerFactory<Object, Object> consumerFactory) {
    return IntegrationFlow.from(
            Kafka.inboundChannelAdapter(consumerFactory, new ConsumerProperties(INTEGRATION_TOPIC) {{
              setGroupId("integration-group");
            }}))
        .channel(inboundChannel)
        .get();
  }

  @Bean
  IntegrationFlow outbound(MessageChannel outboundChannel, MyKafkaTemplate kafkaTemplate) {
    var handler = Kafka.outboundChannelAdapter(kafkaTemplate).topic(INTEGRATION_TOPIC).getObject();
    return IntegrationFlow.from(outboundChannel).handle(handler).get();
  }

  @Bean
  MessageChannel inboundChannel() {
    return MessageChannels.direct().getObject();
  }

  @Bean
  MessageChannel outboundChannel() {
    return MessageChannels.direct().getObject();
  }

  @Bean
  MessageConverter messageConverter() {
    return new JsonMessageConverter();
  }

  @Bean
  MyKafkaTemplate kafkaTemplate(ProducerFactory<Object, Object> producerFactory) {
    return new MyKafkaTemplate(
        producerFactory,
        Map.of(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class));
  }
}

class MyKafkaTemplate extends KafkaTemplate<Object, Object> {

  public MyKafkaTemplate(
      ProducerFactory<Object, Object> producerFactory, Map<String, Object> configs) {
    super(producerFactory, configs);
  }
}

record PV(String pageName, int count) {}
