package com.shu.hbase.Pojo;

import lombok.Data;
import org.springframework.validation.annotation.Validated;

import javax.validation.constraints.NotNull;
import java.util.List;

@Data
@Validated
public class ShareToFileVO {

    private String groupId;

    private String uId;

    private List<String> fileList;
}
