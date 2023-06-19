package com.kafka.stream.work.handler;

import com.kafka.stream.work.entity.FacilityEntity;
import com.kafka.stream.work.processor.HandlerProcessor;
import com.kafka.stream.work.serde.StreamsSerde;
import com.kafka.stream.work.vo.FacilityVo;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.*;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.processor.UsePartitionTimeOnInvalidTimestamp;
import org.apache.kafka.streams.processor.WallclockTimestampExtractor;
import org.apache.kafka.streams.state.*;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;

import java.time.Duration;
import java.util.Properties;

import static org.apache.kafka.streams.StoreQueryParameters.fromNameAndType;

/**
 * @author wuk
 * @description:
 * @menu
 * @date 2023/6/12 18:48
 */
public class HandlerApplication {

    public static void main(String[] args) throws Exception {

        JsonDeserializer<FacilityVo> facilityVoJsonDeserializer = new JsonDeserializer<>(FacilityVo.class);

        JsonSerializer<FacilityEntity> facilityEntityJsonSerializer = new JsonSerializer<>();
        JsonDeserializer<FacilityEntity> facilityEntityJsonDeserializer = new JsonDeserializer<>(FacilityEntity.class);
        Serde<FacilityEntity> facilityEntitySerde = StreamsSerde.FacilitySerde();

        Serde<String> stringSerde = Serdes.String();
        Serializer<String> stringSerializer = stringSerde.serializer();
        Deserializer<String> stringDeserializer = stringSerde.deserializer();


        String standardNode = "standard-node";
        String outOfStandardNode = "out-of-standard-node";
        String processorNodeName = "processor-node-name";
        String purchaseSourceNodeName = "purchase-source-node-name";

        HandlerProcessor handlerProcessor = new HandlerProcessor(standardNode, outOfStandardNode);

        StreamsBuilder builder = new StreamsBuilder();
        //窗口长度
        final Duration windowSize = Duration.ofSeconds(5);
        //窗口滑动间隔
        final Duration advanceBy = Duration.ofSeconds(5);

        //开窗获取”“standard-facility"”，并将数据存储到WindowStore中，统计每个id计数
        //Consumed.wit中序列化与反序列化要与实际“standard-facility”topic主题传的数据格式一致
        KTable<Windowed<String>, Long> standardCount = builder.stream("standard-facility", Consumed.with(stringSerde, facilityEntitySerde).withOffsetResetPolicy(Topology.AutoOffsetReset.LATEST))
                .groupBy((key, value) -> key, Grouped.with("standard-facility-window", stringSerde, facilityEntitySerde))
                .windowedBy(TimeWindows.of(windowSize).advanceBy(advanceBy))
                .count(Materialized.<String, Long, WindowStore<Bytes, byte[]>>as("StandardCount"));

        //开窗获取”“standard-facility"”
//        KTable<Windowed<FacilityEntity>, Long> outOfStandardCount = builder.stream("out-of-standard-facility", Consumed.with(stringSerde,StreamsSerde.FacilitySerde()).withOffsetResetPolicy(Topology.AutoOffsetReset.EARLIEST))
//                .groupBy((key, facilityEntity) -> facilityEntity)
//                .windowedBy(TimeWindows.of(windowSize).advanceBy(advanceBy)).count();

        //[apple=5] @ 1623245792000
        //[banana=3] @ 1623245793000
        //[orange=8] @ 1623245794000
        //输出
        standardCount.toStream().print(Printed.<Windowed<String>, Long>toSysOut()
                .withLabel("Customer Transactions Counts"));
//        outOfStandardCount.toStream(). map((key, value) -> new KeyValue<>(key.toString(), value)).print(Printed.toSysOut());

        Topology topology = builder.build();

        //1-读取kafka数据方式；2-节点名称 ；3- 可选的时间戳提取器。用于从消息中提取时间戳，以便进行基于时间的操作【UsePartitionTimeOnInvalidTimestamp是Kafka Streams的一个配置属性，用于处理无效时间戳的消息。】；
        // 4-key反序列化 ；5-value反序列化； 6-订阅数据来源主题；
        topology.addSource(Topology.AutoOffsetReset.EARLIEST, purchaseSourceNodeName,
                        new UsePartitionTimeOnInvalidTimestamp(), stringDeserializer, facilityVoJsonDeserializer, "facility-messages")
                //1-处理器名称；2-处理器方法；3-处理器父节点
                .addProcessor(processorNodeName, () -> handlerProcessor, purchaseSourceNodeName)
                //1-接收器名称；2-该接收器代表的主题；3-key序列化器；4-value序列化器；5-接收器父节点
                .addSink(standardNode, "standard-facility", stringSerializer, facilityEntitySerde.serializer(), processorNodeName);
        // .addSink(outOfStandardNode, "out-of-standard-facility", stringSerializer,  stringSerializer, processorNodeName);

        //最简单的测试Topology
        //topology.addSource(Topology.AutoOffsetReset.EARLIEST, purchaseSourceNodeName,"facility-messages")
        //.addProcessor(processorNodeName, () -> handlerProcessor, purchaseSourceNodeName);;

        StreamsConfig streamsConfig = new StreamsConfig(getProperties());
        KafkaStreams kafkaStreams = new KafkaStreams(topology, streamsConfig);
        System.out.println("Starting CountingWindowing and KTableJoins Example");
        kafkaStreams.cleanUp();
        kafkaStreams.start();
        Thread.sleep(30000);
        //设置交互式查询
        System.out.println("Query has ready,Please read message on the console");
        //因为前面用的是窗口存储，所以此处也要使用窗口类型
        ReadOnlyWindowStore<String, Long> standardStore = kafkaStreams.store(fromNameAndType("StandardCount", QueryableStoreTypes.windowStore()));
        KeyValueIterator<Windowed<String>, Long> range = standardStore.all();
        while (range.hasNext()) {
            System.out.println("thread has start,Please read message on the console");
            KeyValue<Windowed<String>, Long> next = range.next();
            //原键格式是：数据键+时间戳，我们现只获取数据键
            System.out.println("Count for" + next.key.key() + ":" + next.value);
        }
        System.out.println("Shutting down the CountingWindowing and KTableJoins Example Application now");
        kafkaStreams.close();
    }

    private static Properties getProperties() {
        Properties props = new Properties();
        //设置Kafka客户端的唯一标识符
        props.put(StreamsConfig.CLIENT_ID_CONFIG, "facility-app-test");
        //用于设置Kafka消费者组的标识符
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "facility-app-group");
        // 用于设置Kafka Streams应用程序的唯一标识符
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "facility-app-appid");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.0.35:9092");
        props.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, 1);
        //"earliest"：消费者将从最早的可用偏移量开始消费。即，如果消费者组首次消费主题，或者发生偏移重置时，将从主题的最早消息开始消费。
        //"latest"：消费者将从最新的偏移量开始消费。即，如果消费者组首次消费主题，或者发生偏移重置时，将从主题的最新消息开始消费。
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        //设置自动提交offset
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        //设置提交间隔
        //props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG,"1000");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, WallclockTimestampExtractor.class);
        return props;
    }

    private static KeyValueMapper<String, FacilityEntity, KeyValue<String, FacilityEntity>> getKeyValueMapper() {
        return (k, v) -> KeyValue.pair(v.getMacId(), v);
    }
}
