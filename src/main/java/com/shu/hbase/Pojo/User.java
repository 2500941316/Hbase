package com.shu.hbase.Pojo;

import lombok.Data;

import java.io.Serializable;

@Data
public class User implements Serializable {
    //**工号**,姓名,部门,身份

    private String userId;
    private String name;
    private String department;//本人所在部门
    private String duty;//身份


}
