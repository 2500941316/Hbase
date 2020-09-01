package com.shu.hbase.Controller;

import com.shu.hbase.Pojo.Static;
import com.shu.hbase.Tools.HbasePool.HbaseConnectionPool;
import com.shu.hbase.Tools.HdfsPool.HdfsConnectionPool;
import com.shu.hbase.Tools.Post;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.GetMapping;

import java.io.IOException;

@Controller
@CrossOrigin
public class TestController {
    /**
     * 输出file表的数据
     *
     * @param
     * @throws IOException
     */
    @GetMapping("filesTable")
    public void fileTable() throws Exception {
        Connection connection = HbaseConnectionPool.getHbaseConnection();
        Table table = connection.getTable(TableName.valueOf(Static.FILE_TABLE));
        ResultScanner scanner = table.getScanner(new Scan().setMaxVersions());

        System.out.println("file表的数据为：-------------------------------------------------------");
        for (Result result : scanner) {
            System.out.println("新的一行" + Bytes.toString(result.getRow()));
            for (Cell cell : result.rawCells()) {
                System.out.print("列名为" + Bytes.toString(CellUtil.cloneQualifier(cell)) + "  ");
                System.out.println(Bytes.toString(CellUtil.cloneValue(cell)));
            }
        }
        HbaseConnectionPool.releaseConnection(connection);
    }

    /**
     * 测试index表的数据
     *
     * @param
     * @throws IOException
     */
    @GetMapping("indexTable")
    public void indexTable() throws Exception {
        Connection connection = HbaseConnectionPool.getHbaseConnection();
        Table table = connection.getTable(TableName.valueOf(Static.INDEX_TABLE));
        ResultScanner scanner = table.getScanner(new Scan().setMaxVersions());
        System.out.println("index表的数据为：-------------------------------------------------------");
        if (scanner != null) {
            for (Result result : scanner) {
                System.out.println("新的一行" + Bytes.toString(result.getRow()));
                for (Cell cell : result.rawCells()) {
                    System.out.print("列名为" + Bytes.toString(CellUtil.cloneQualifier(cell)) + "  ");
                    System.out.println(Bytes.toString(CellUtil.cloneValue(cell)));
                }
            }
        }


         HbaseConnectionPool.releaseConnection(connection);
    }

    /**
     * 测试group表的数据
     *
     * @param
     * @throws IOException
     */
    @GetMapping("groupTable")
    public void groupTable() throws Exception {
        Connection connection = HbaseConnectionPool.getHbaseConnection();
        Table table = connection.getTable(TableName.valueOf(Static.GROUP_TABLE));
        ResultScanner scanner = table.getScanner(new Scan().setMaxVersions());
        System.out.println("group表的数据为：-------------------------------------------------------");
        for (Result result : scanner) {
            System.out.println("新的一行" + Bytes.toString(result.getRow()));
            for (Cell cell : result.rawCells()) {
                System.out.print("列名为" + Bytes.toString(CellUtil.cloneQualifier(cell)) + "  ");
                System.out.println(Bytes.toString(CellUtil.cloneValue(cell)));
            }
        }
        HbaseConnectionPool.releaseConnection(connection);
    }

    /**
     * 测试user表的数据
     *
     * @param
     * @throws IOException
     */
    @GetMapping("userTable")
    public void buildDirect() throws Exception {
        Connection connection = HbaseConnectionPool.getHbaseConnection();
        Table table = connection.getTable(TableName.valueOf(Static.USER_TABLE));
        ResultScanner scanner = table.getScanner(new Scan().setMaxVersions());
        System.out.println("user表的数据为：-------------------------------------------------------");
        for (Result result : scanner) {
            System.out.println("新的一行" + Bytes.toString(result.getRow()));
            for (Cell cell : result.rawCells()) {
                System.out.print("列名为" + Bytes.toString(CellUtil.cloneQualifier(cell)) + "  ");
                System.out.println(Bytes.toString(CellUtil.cloneValue(cell)));
            }
        }
         HbaseConnectionPool.releaseConnection(connection);
    }

    /**
     * 删除user表
     *
     * @param
     * @throws IOException
     */
    @GetMapping("deleteUserTable")
    public void deleteUserTable() throws Exception {
        Manager.deleteTable("shuwebfs:user");
    }

    /**
     * 创建user表
     *
     * @param
     * @throws IOException
     */
    @GetMapping("buildUserTable")
    public void buildUserTable() throws Exception {
        Manager.buildTable(Static.USER_TABLE, new String[]{Static.USER_TABLE_CF});
    }


    /**
     * 查询hdfs的文件
     *
     * @param
     * @throws IOException
     */
    @GetMapping("getHdfs")
    public void getHdfs() throws Exception {
        FileSystem fs = HdfsConnectionPool.getHdfsConnection();
        FileStatus[] fileStatuses = fs.listStatus(new Path("/shuwebfs/19721631/"));
        for (FileStatus fileStatus : fileStatuses) {
            System.out.println(fileStatus.getPath().getName());
        }
        HdfsConnectionPool.releaseConnection(fs);
    }

    /**
     * 删除所有表的数据
     *
     * @param
     * @throws IOException
     */
    @GetMapping("deleteAll")
    public void deleteAll() throws Exception {
        Manager.clearTables();
    }

    /**
     * 删除hdfs下的所以文件
     *
     * @param
     * @throws IOException
     */
    @GetMapping("deleteHdfs")
    public void deleteHdfs() throws Exception {
        FileSystem fs = HdfsConnectionPool.getHdfsConnection();
        fs.delete(new Path("/shuwebfs/19721631/"), true);
         HdfsConnectionPool.releaseConnection(fs);
    }

    /**
     * 给首页传值
     *
     * @param
     * @throws IOException
     */
    @GetMapping("postToindex")
    public void postToindex() throws Exception {
        Post.Post();
    }
}
