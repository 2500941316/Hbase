package com.shu.hbase.Pojo;

import lombok.Data;

import java.util.List;

@Data
public class MyGroupInfo {
    private String name;
    private List<String> member;
    private List<String> filePath;
}
