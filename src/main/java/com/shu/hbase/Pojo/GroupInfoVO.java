package com.shu.hbase.Pojo;

import lombok.Data;

import java.util.Set;

@Data
public class GroupInfoVO {
    private String gId;
    private String name;
    private Set<String> member;
    private Set<ShareFileVO> file;

}
