package com.shu.hbase.Pojo;

import lombok.Data;

import java.util.List;

@Data
public class NewGroupInfoVO {
    private String uId;
    private String groupName;
    private List<String> member;
}
