package com.kafka.stream.work.vo;

import lombok.Data;

import java.math.BigDecimal;

/**
 * @author wuk
 * @description:
 * @menu
 * @date 2023/6/12 15:59
 */
@Data
public class FacilityVo {

    private String macId;

    private String facilityName;

    private BigDecimal deepNum;
}
