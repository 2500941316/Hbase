package com.shu.hbase.Pojo.Api;

import lombok.Data;

@Data
public class ApiFileVo {
    private String fileId;
    private String name;
    private Long size;
    private String type;
    private Long time;
}
