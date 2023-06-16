package com.kafka.stream.work.test;

import org.apache.kafka.clients.producer.*;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * @author wuk
 * @description:
 * @menu
 * @date 2023/6/14 17:18
 */
public class CustomProducer {
        //kafka集群机器
        private static final String KAFKA_HOSTS = "192.168.0.35:9092";
        //topic名称
        private static final String TOPIC = "my-replicated-topic_2";

        public static void main(String[] args) {
            Properties props = new Properties();
            //kafka 集群，broker-list
            props.put("bootstrap.servers", KAFKA_HOSTS);
            props.put("acks", "all");
            //重试次数
            props.put("retries", 1);
            //批次大小
            props.put("batch.size", 16384);
            //等待时间
            props.put("linger.ms", 1);
            //RecordAccumulator 缓冲区大小
            props.put("buffer.memory", 33554432);
            props.put("key.serializer",
                    "org.apache.kafka.common.serialization.StringSerializer");
            props.put("value.serializer",
                    "org.apache.kafka.common.serialization.StringSerializer");
            Producer<String, String> producer = new
                    KafkaProducer<>(props);
            for (int i = 150000; i < 1500; i++) {
                producer.send(new ProducerRecord<String, String>("facility-messages",
                        Integer.toString(i), Integer.toString(i)));
            }
            producer.close();
        }
}
