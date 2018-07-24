package com.howell.spark.bean;

import lombok.Data;

@Data
public class DB_User {
    private int id;
    private String name;
    private int age;
    private int sex;
    private String cid; //身份证号
}
