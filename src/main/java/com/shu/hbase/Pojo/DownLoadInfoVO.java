package com.shu.hbase.Pojo;

import lombok.Data;
import org.springframework.validation.annotation.Validated;

import javax.validation.constraints.NotNull;

@Data
@Validated
public class DownLoadInfoVO {

    @NotNull
    private String detSrc;

    @NotNull
    private String localSrc;
}
