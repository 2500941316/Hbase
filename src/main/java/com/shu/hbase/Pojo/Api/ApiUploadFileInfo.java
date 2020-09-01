package com.shu.hbase.Pojo.Api;

import lombok.Data;

@Data
public class ApiUploadFileInfo {
    private String userId;
    private String fileName;
    private String fileStr;
    private String time;
    private String key;

}
