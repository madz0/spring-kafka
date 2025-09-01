package com.github.madz0.sdkafka.alltogether;

import static com.github.madz0.sdkafka.alltogether.SdKafkaApplication.PAGE_VIEWS_TOPIC;

import java.util.Map;
import java.util.Random;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.IntStream;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.cloud.stream.function.StreamBridge;
import org.springframework.context.ApplicationListener;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.context.annotation.PropertySource;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.dsl.MessageChannels;
import org.springframework.integration.kafka.dsl.Kafka;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.converter.JsonMessageConverter;
import org.springframework.kafka.support.converter.MessageConverter;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.support.MessageBuilder;

@SpringBootApplication
public class SdKafkaApplication {

  static final String PAGE_VIEWS_TOPIC = "pv-topic";
  static final String PAGE_VIEWS_GROUP = "pv-topic";

  public static void main(String[] args) {
    SpringApplication.run(SdKafkaApplication.class, args);
  }

  /*
  see https://kafka.apache.org/37/documentation/streams/developer-guide/dsl-api.html
   */
  @Bean
  Function<KStream<String, PageView>, KTable<String, Long>> counter() {
    return pvs ->
        pvs.filter((Kafka, v) -> v.duration() >= 100)
            .map((k, v) -> new KeyValue<>(v.page(), 0L))
            .groupByKey(Grouped.with(Serdes.String(), Serdes.Long()))
            .count(Materialized.as("pcmv"));
  }

  @Bean
  Consumer<KTable<String, Long>> logger() {
    return counts ->
        counts.toStream().foreach((page, count) -> System.out.println(page + ":" + count));
  }
}

@Configuration
@PropertySource("classpath:all-together.properties")
class RunnerConfiguration {

  private static PageView randomPageView(String source) {

    var names = "mo,will,ali,vali".split(",");
    var pages = "dashboard,account,balance,index".split(",");
    var random = new Random();
    return new PageView(
        pages[random.nextInt(pages.length)],
        Math.random() > .5 ? 100 : 1000,
        names[random.nextInt(names.length)],
        source);
  }

  void kafka(KafkaTemplate<Object, Object> kafkaTemplate) {
    kafkaTemplate.send(PAGE_VIEWS_TOPIC, randomPageView("kafka"));
  }

  void integration(MessageChannel messageChannel) {
    messageChannel.send(
        MessageBuilder.withPayload(randomPageView("integration"))
            // how to set topic dynamically
            // .copyHeadersIfAbsent(Map.of(KafkaHeaders.TOPIC, PAGE_VIEWS_TOPIC))
            .build());
  }

  void stream(StreamBridge streamBridge) {
    streamBridge.send(PAGE_VIEWS_TOPIC, randomPageView("stream"));
  }

  @Bean
  ApplicationListener<ApplicationReadyEvent> applicationListener(
      KafkaTemplate<Object, Object> kafkaTemplate,
      MessageChannel messageChannel,
      StreamBridge streamBridge) {
    return a ->
        IntStream.range(0, 1000)
            .forEach(
                i -> {
                  kafka(kafkaTemplate);
                  integration(messageChannel);
                  stream(streamBridge);
                });
  }
}

@Configuration
class IntegrationConfiguration {

  @Bean
  IntegrationFlow integrationFlow(
      MessageChannel messageChannel, KafkaTemplate<Object, Object> kafkaTemplate) {
    var handler =
        Kafka.outboundChannelAdapter(kafkaTemplate)
            // you can skip setting topic in here and specify it in message header
            .topic(PAGE_VIEWS_TOPIC)
            .getObject();
    return IntegrationFlow.from(messageChannel).handle(handler).get();
  }

  @Bean
  @Primary
  MessageChannel channel() {
    return MessageChannels.direct().getObject();
  }
}

/*
First step is low level kafka
 */
@Configuration
class KafkaConfiguration {

  @KafkaListener(topics = PAGE_VIEWS_TOPIC, groupId = SdKafkaApplication.PAGE_VIEWS_GROUP)
  void onNewPageView(Message<PageView> message) {
    /*System.out.println("-------------");
    System.out.println("onPageView received page view" + message.getPayload());
    message.getHeaders().forEach((k, v) -> System.out.println(k + "=" + v));*/
  }

  @Bean
  NewTopic pageViewsTopic() {
    return new NewTopic(PAGE_VIEWS_TOPIC, 1, (short) 1);
  }

  @Bean
  MessageConverter messageConverter() {
    return new JsonMessageConverter();
  }

  @Bean
  KafkaTemplate<Object, Object> kafkaTemplate(ProducerFactory<Object, Object> producerFactory) {
    return new KafkaTemplate<>(
        producerFactory,
        Map.of(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class));
  }
}

record PageView(String page, long duration, String userId, String source) {}
