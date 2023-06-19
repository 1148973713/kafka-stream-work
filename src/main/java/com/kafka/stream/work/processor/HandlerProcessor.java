package com.kafka.stream.work.processor;

import com.alibaba.fastjson.JSON;
import com.kafka.stream.work.entity.FacilityEntity;
import com.kafka.stream.work.vo.FacilityVo;
import org.apache.kafka.streams.processor.AbstractProcessor;
import org.apache.kafka.streams.processor.To;
import org.apache.kafka.streams.processor.api.ContextualProcessor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.springframework.beans.BeanUtils;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.Date;

/**
 * @author wuk
 * @description:
 * @menu
 * @date 2023/6/12 15:34
 */
public class HandlerProcessor extends AbstractProcessor<String, FacilityVo> {

    private static final BigDecimal STANDARD_NUM = BigDecimal.valueOf(520.123);

    private String standardNode;

    private String outOfStandardNode;

    public HandlerProcessor(String StandardNode, String OutOfStandardNode) {
        this.standardNode = StandardNode;
        this.outOfStandardNode = OutOfStandardNode;
    }

//    @Override
//    public void init(final ProcessorContext<String,FacilityEntity> context) {
//        super.init(context);
//        // 在这里进行任何初始化操作
//    }

    /*@Override
    public void process(final Record<String, FacilityVo> record) {
        FacilityVo facilityVo = record.value();
       String key = record.key();

        int result = facilityVo.getDeepNum().compareTo(STANDARD_NUM);

        FacilityEntity facilityEntity = new FacilityEntity();
        BeanUtils.copyProperties(facilityVo,facilityEntity);

        Date date = new Date();
        long time = date.getTime();

        if (result<0){
            facilityEntity.setWarnMessage("水位线已经超出预警标准!");
            facilityEntity.setIsWarn(true);
            Record<String, FacilityEntity> entityRecord = new Record<>(key, facilityEntity, time);
            context().forward(entityRecord,outOfStandardNode);
        }else {
            facilityEntity.setWarnMessage("水位线安全!");
            facilityEntity.setIsWarn(false);
            Record<String, FacilityEntity> entityRecord = new Record<>(key, facilityEntity, time);
            context().forward(entityRecord,standardNode);
        }
    }*/

    @Override
    public void process(String key, FacilityVo facilityVo) {
        //FacilityVo facilityVo = JSON.parseObject(value, FacilityVo.class);
        //System.out.println("------------------------------>print:"+key + ":" + facilityVo.toString());
        int result = facilityVo.getDeepNum().compareTo(STANDARD_NUM);

        FacilityEntity facilityEntity = new FacilityEntity();
        BeanUtils.copyProperties(facilityVo,facilityEntity);

        Date date = new Date();
        long time = date.getTime();

        if (result<0){
            facilityEntity.setWarnMessage(key+"号水位线已经超出预警标准!");
            facilityEntity.setIsWarn(true);
            System.out.println(key+"号一个水位超标");

            String jsonString = JSON.toJSONString(facilityEntity);
            //转发到节点
            context().forward(key,facilityEntity,To.child(standardNode));
        }else {
            facilityEntity.setWarnMessage("水位线安全!");
            facilityEntity.setIsWarn(false);
            Record<String, FacilityEntity> entityRecord = new Record<>(key, facilityEntity, time);
            System.out.println(key+"号一个水位没有超标!");

            String jsonString = JSON.toJSONString(facilityEntity);
            //转发到节点
            //context().forward(key,jsonString,To.child(outOfStandardNode));
        }
    }
}
