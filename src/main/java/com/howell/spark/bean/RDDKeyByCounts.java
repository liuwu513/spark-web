package com.howell.spark.bean;

import lombok.Data;

import java.io.Serializable;

@Data
public class RDDKeyByCounts implements Serializable {
    private String name;
    private Integer counts;
    private String goods_category;
}
