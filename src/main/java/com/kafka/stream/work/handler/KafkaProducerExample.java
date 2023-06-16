package com.kafka.stream.work.handler;

import com.alibaba.fastjson.JSON;
import com.kafka.stream.work.vo.FacilityVo;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;

import java.math.BigDecimal;
import java.util.Properties;
import java.util.Random;
import java.util.UUID;

/**
 * @author wuk
 * @description:
 * @menu
 * @date 2023/6/13 21:56
 */
public class KafkaProducerExample {
    public static void main(String[] args) throws InterruptedException {

        // 创建生产者配置
        Properties props = new Properties();
        props.put("bootstrap.servers", "192.168.0.35:9092");
        props.put("key.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");

        // 创建生产者实例
        Producer<String, String> producer = new KafkaProducer<>(props);

        for (int i = 2030; i <= 2050; i++) {
            // 构建要发送的消息记录
            String topic = "facility-messages";
            String key = String.valueOf(i);
            FacilityVo facilityVo = new FacilityVo();
            facilityVo.setFacilityName("TianHe WaterFacility");
            facilityVo.setMacId(UUID.randomUUID().toString());
            //生成随机数
            Random random = new Random();
            // 生成一个范围在1到1000之间的随机整数
            int randomNumber = random.nextInt(1000) + 1;
            facilityVo.setDeepNum(BigDecimal.valueOf(randomNumber));

            String facilityVoJSON = JSON.toJSONString(facilityVo);

            ProducerRecord<String,String> record = new ProducerRecord<>(topic, key, facilityVoJSON);

            //休息一秒测试开窗操作
            Thread.sleep(5000);
            // 发送消息
            producer.send(record, new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                     if (exception!=null){
                        exception.printStackTrace();
                     }
                }
            });
        }
        // 关闭生产者
        producer.close();
    }

}
