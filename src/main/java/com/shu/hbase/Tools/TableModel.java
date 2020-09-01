package com.shu.hbase.Tools;

import lombok.Data;

import java.util.ArrayList;

@Data
public class TableModel<T> {
    private int code;
    private String msg;
    private int count;
    private Object data;


    public static TableModel success(Object data, int total) {
        TableModel tableModel = new TableModel();

        tableModel.setCode(200);
        tableModel.setMsg(null);
        tableModel.setCount(total);
        tableModel.setData(data);

        return tableModel;
    }

    public static TableModel success(String msg) {
        TableModel tableModel = new TableModel();

        tableModel.setCode(200);
        tableModel.setMsg(msg);
        tableModel.setCount(0);

        return tableModel;
    }

    public static TableModel error(String message) {
        TableModel tableModel = new TableModel();
        tableModel.setCode(500);
        tableModel.setData(new ArrayList<>());
        tableModel.setMsg(message);
        tableModel.setCount(0);

        return tableModel;
    }

    public static TableModel error(Integer code, String msg) {
        TableModel tableModel = new TableModel();
        tableModel.setCode(code);
        tableModel.setMsg(msg);

        return tableModel;
    }
}
