package com.shu.hbase.Pojo;

import lombok.Data;

import java.util.ArrayList;
import java.util.List;

@Data
public class QuestionSelectInfo {
    private String qId;
    private String quer;
    private String title;
    private String type;
    private Long time;
    private String content;
    private int answerNum;
    private List<AnswerInfo> list;

    public QuestionSelectInfo() {
        this.list = new ArrayList<>();
    }

}
