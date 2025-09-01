package com.github.madz0.sdkafka.kafkastream;

import java.util.Random;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.IntStream;
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
import org.springframework.context.annotation.PropertySource;

@SpringBootApplication
public class Test {
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

  public static void main(String[] args) {
    SpringApplication.run(Test.class, args);
  }

  @Bean
  ApplicationListener<ApplicationReadyEvent> applicationListener(StreamBridge streamBridge) {
    return e ->
        IntStream.range(0, 1000)
            .forEach(
                i -> {
                  streamBridge.send(
                      "pv-topic",
                      randomPageView("kafka-stream"));
                });
  }
}

@Configuration
@PropertySource("classpath:kafka-stream-config.properties")
class KafkaStreamConfiguration {
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

record PageView(String page, long duration, String userId, String source) {}
