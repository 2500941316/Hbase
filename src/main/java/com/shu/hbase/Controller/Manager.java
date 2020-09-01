package com.shu.hbase.Controller;
import com.shu.hbase.Pojo.Static;
import com.shu.hbase.Tools.HbasePool.HbaseConnectionPool;
import com.shu.hbase.Tools.HdfsPool.HdfsConnectionPool;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class Manager {
    public static void main(String[] args) throws Exception {
     //tableInfo(Static.FILE_TABLE);
       // FileSystem hdfsConnection = HdfsConnectionPool.getHdfsConnection();
       // boolean exists = hdfsConnection.exists(new Path("/shuwebfs/19721631/我的文档"));
       // System.out.println(exists);

        FileSystem hdfsConnection = HdfsConnectionPool.getHdfsConnection();
        System.out.println(hdfsConnection.exists(new Path("/shuwebfs/19721631/我的文档")));
        System.out.println(hdfsConnection.exists(new Path("/11")));

    }



    public static void clearRoot() throws Exception {
        Manager.buildTable(Static.USER_TABLE,new String[]{Static.USER_TABLE_CF});
        Manager.buildTable(Static.FILE_TABLE,new String[]{Static.FILE_TABLE_CF});
        Manager.buildTable(Static.GROUP_TABLE,new String[]{Static.GROUP_TABLE_CF});
        Manager.buildTable(Static.INDEX_TABLE,new String[]{Static.INDEX_TABLE_CF});

        FileSystem hdfsConnection = HdfsConnectionPool.getHdfsConnection();
        hdfsConnection.delete(new Path("/shuwebfs/19721631/"),true);
        hdfsConnection.delete(new Path("/shuwebfs/19721632/"),true);
        hdfsConnection.delete(new Path("/shuwebfs/test/"),true);
        hdfsConnection.delete(new Path("/shuwebfs/test1/"),true);
    }

    public static void clearTables() throws Exception {
        Connection connection = HbaseConnectionPool.getHbaseConnection();
        Table table1 = connection.getTable(TableName.valueOf(Static.FILE_TABLE));
        Table table2 = connection.getTable(TableName.valueOf(Static.GROUP_TABLE));
        Table table3 = connection.getTable(TableName.valueOf(Static.INDEX_TABLE));
        Table table4 = connection.getTable(TableName.valueOf(Static.USER_TABLE));
        Scan scan=new Scan();

        Iterator<Result> iterator1 = table1.getScanner(scan).iterator();
        listDeleteRow(iterator1,table1);
        Iterator<Result> iterator2 = table2.getScanner(scan).iterator();
        listDeleteRow(iterator2,table2);
        Iterator<Result> iterator3 = table3.getScanner(scan).iterator();
        listDeleteRow(iterator3,table3);
        Iterator<Result> iterator4 = table4.getScanner(scan).iterator();
        listDeleteRow(iterator4,table4);
        HbaseConnectionPool.releaseConnection(connection);
    }

    public static void listDeleteRow(Iterator<Result> iterator,Table table) throws Exception {
        List<Delete> deleteList=new ArrayList<>();
        while (iterator.hasNext())
        {
            Result result = iterator.next();

            Delete delete=new Delete(result.getRow());
            deleteList.add(delete);
        }
        table.delete(deleteList);
        System.out.println(table.getName().toString()+"删除了数据数量为："+deleteList.size());
    }


    public static void listTableNames() throws Exception {
        Connection connection = HbaseConnectionPool.getHbaseConnection();
        Admin admin = connection.getAdmin();
        TableName[] tableNames = admin.listTableNames();
        for (TableName tableName : tableNames) {
            System.out.println(tableName.toString());
        }
        HbaseConnectionPool.releaseConnection(connection);
    }

    public static void tableInfo(String tableName) throws Exception {
        Connection connection = HbaseConnectionPool.getHbaseConnection();
        Table table = connection.getTable(TableName.valueOf(tableName));
        ResultScanner scanner = table.getScanner(new Scan());
        for (Result result : scanner) {
            System.out.println(Bytes.toString(result.getRow()));
            for (Cell cell : result.rawCells()) {
                System.out.print(Bytes.toString(CellUtil.cloneQualifier(cell))+"  ");
                System.out.println(Bytes.toString(CellUtil.cloneValue(cell)));
            }
        }
        HbaseConnectionPool.releaseConnection(connection);
    }

    public static void buildTable(String tableName, String columnFamily[]) throws Exception {
        Connection connection = HbaseConnectionPool.getHbaseConnection();
        Admin admin = connection.getAdmin();
        // 添加列族
        HTableDescriptor desc = new HTableDescriptor(TableName.valueOf(tableName));
        for (int i = 0; i < columnFamily.length; i++) {
            desc.addFamily(new HColumnDescriptor(columnFamily[i]).setMaxVersions(1));

        }

        // 如果表存在就是先disable，然后在delete
        if (admin.tableExists(TableName.valueOf(tableName))) {
            admin.disableTable(TableName.valueOf(tableName));
            admin.deleteTable(TableName.valueOf(tableName));
        }
        // 创建表
        admin.createTable(desc);
        HbaseConnectionPool.releaseConnection(connection);
    }


    public static void deleteTable(String tableName) throws Exception {
        Connection connection = HbaseConnectionPool.getHbaseConnection();
            Admin admin = connection.getAdmin();
        if (admin.tableExists(TableName.valueOf(tableName))) {
            admin.disableTable(TableName.valueOf(tableName));
            admin.deleteTable(TableName.valueOf(tableName));
        } else {
            System.out.println("table \"" + tableName + "\" is not exist!");
        }
        HbaseConnectionPool.releaseConnection(connection);
    }
}
