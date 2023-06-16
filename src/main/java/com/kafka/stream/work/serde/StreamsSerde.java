package com.kafka.stream.work.serde;

import com.kafka.stream.work.entity.FacilityEntity;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;

/**
 * @author wuk
 * @description:
 * @menu
 * @date 2023/6/12 23:43
 */
public class StreamsSerde<T>{

    public static Serde<FacilityEntity> FacilitySerde(){
        return new FacilitySerde();
    }

    public static final class FacilitySerde extends Serdes.WrapperSerde<FacilityEntity> {
        public FacilitySerde() {
            super(new JsonSerializer<>(), new JsonDeserializer<>(FacilityEntity.class));
        }
    }
}
