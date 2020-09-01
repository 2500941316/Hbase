package com.shu.hbase.Pojo;

import lombok.Data;

@Data
public class ShareFileVO {
    private String name;
    private String size;
    private String path;
    private String type;
    private boolean isDir;
    private String sharer;
    private boolean isMyShare;

}
