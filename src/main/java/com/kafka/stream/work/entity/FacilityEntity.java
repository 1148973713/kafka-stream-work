package com.kafka.stream.work.entity;

import lombok.Data;

import java.math.BigDecimal;

/**
 * @author wuk
 * @description:
 * @menu
 * @date 2023/6/12 15:35
 */
@Data
public class FacilityEntity {
    private String macId;

    private String facilityName;

    private BigDecimal deepNum;

    private Boolean isWarn;

    private String warnMessage;
}
