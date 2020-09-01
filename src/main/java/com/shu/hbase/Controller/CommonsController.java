package com.shu.hbase.Controller;

import com.shu.hbase.Pojo.Api.ApiDownLoadInfo;
import com.shu.hbase.Pojo.Api.ApiSearchFileInfo;
import com.shu.hbase.Pojo.Api.ApiUploadFileInfo;
import com.shu.hbase.Pojo.Static;
import com.shu.hbase.Pojo.Api.ApiFileVo;
import com.shu.hbase.Tools.API.Base64Tool;
import com.shu.hbase.Tools.HbasePool.HbaseConnectionPool;
import com.shu.hbase.Tools.HdfsPool.HdfsConnectionPool;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IOUtils;
import org.springframework.web.bind.annotation.*;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

@RestController
@CrossOrigin
@RequestMapping("commonAPI")
public class CommonsController {
    @PostMapping("uploadFile")
    public boolean uploadFile(@RequestBody ApiUploadFileInfo commonsFileInfoVO) {

        FileSystem fs = null;
        Connection hBaseConn = null;
        Table fileTable = null;
        if (commonsFileInfoVO.getUserId().isEmpty()
                || commonsFileInfoVO.getFileStr().isEmpty()) {
            return false;
        }
        try {
            fs = HdfsConnectionPool.getHdfsConnection();
            hBaseConn = HbaseConnectionPool.getHbaseConnection();
            fileTable = hBaseConn.getTable(TableName.valueOf(Static.FILE_TABLE));
            String uId = commonsFileInfoVO.getUserId();
            String fileName = commonsFileInfoVO.getFileName();
            byte[] decode = Base64Tool.Base64ToFile(commonsFileInfoVO.getFileStr());
            //插入files表中
            Put put = new Put(Bytes.toBytes(commonsFileInfoVO.getUserId() + "_" + System.currentTimeMillis()));
            put.addColumn(Bytes.toBytes(Static.FILE_TABLE_CF), Bytes.toBytes(Static.FILE_TABLE_NAME), Bytes.toBytes(commonsFileInfoVO.getFileName()));
            put.addColumn(Bytes.toBytes(Static.FILE_TABLE_CF), Bytes.toBytes(Static.FILE_TABLE_SIZE), Bytes.toBytes("-"));
            put.addColumn(Bytes.toBytes(Static.FILE_TABLE_CF), Bytes.toBytes(Static.FILE_TABLE_ISDIR), Bytes.toBytes("false"));
            put.addColumn(Bytes.toBytes(Static.FILE_TABLE_CF), Bytes.toBytes(Static.FILE_TABLE_BACK), Bytes.toBytes(uId));
            put.addColumn(Bytes.toBytes(Static.FILE_TABLE_CF), Bytes.toBytes(Static.FILE_TABLE_PATH), Bytes.toBytes(Static.BASEURL + uId + "/" + fileName));
            put.addColumn(Bytes.toBytes(Static.FILE_TABLE_CF), Bytes.toBytes(Static.FILE_TABLE_Auth), Bytes.toBytes(uId));
            put.addColumn(Bytes.toBytes(Static.FILE_TABLE_CF), Bytes.toBytes(Static.FILE_TABLE_TIME), Bytes.toBytes(System.currentTimeMillis() + ""));
            fileTable.put(put);
            //文件传入hdfs

            FSDataOutputStream out = fs.create(new Path(Static.BASEURL + commonsFileInfoVO.getUserId() + "/" + commonsFileInfoVO.getFileName()));
            out.write(decode);

            out.flush();
            out.close();
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        } finally {
            try {
                if (fs != null || hBaseConn != null) {
                    HdfsConnectionPool.releaseConnection(fs);
                    HbaseConnectionPool.releaseConnection(hBaseConn);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return true;
    }


    @PostMapping("shareToAll")
    public boolean shareToAll(@RequestBody ApiUploadFileInfo commonsFileInfoVO) {
        FileSystem fs = null;
        Connection hBaseConn = null;
        Table fileTable = null;
        if (commonsFileInfoVO.getUserId().isEmpty()
                || commonsFileInfoVO.getFileStr().isEmpty()) {
            return false;
        }
        try {
            fs = HdfsConnectionPool.getHdfsConnection();
            hBaseConn = HbaseConnectionPool.getHbaseConnection();
            fileTable = hBaseConn.getTable(TableName.valueOf(Static.FILE_TABLE));
            String uId = commonsFileInfoVO.getUserId();
            String fileName = commonsFileInfoVO.getFileName();
            byte[] decode = Base64Tool.Base64ToFile(commonsFileInfoVO.getFileStr());
            //插入files表中
            Put put = new Put(Bytes.toBytes("00000000_" + System.currentTimeMillis()));
            put.addColumn(Bytes.toBytes(Static.FILE_TABLE_CF), Bytes.toBytes(Static.FILE_TABLE_NAME), Bytes.toBytes(commonsFileInfoVO.getFileName()));
            put.addColumn(Bytes.toBytes(Static.FILE_TABLE_CF), Bytes.toBytes(Static.FILE_TABLE_SIZE), Bytes.toBytes("-"));
            put.addColumn(Bytes.toBytes(Static.FILE_TABLE_CF), Bytes.toBytes(Static.FILE_TABLE_ISDIR), Bytes.toBytes("false"));
            put.addColumn(Bytes.toBytes(Static.FILE_TABLE_CF), Bytes.toBytes(Static.FILE_TABLE_BACK), Bytes.toBytes(uId));
            put.addColumn(Bytes.toBytes(Static.FILE_TABLE_CF), Bytes.toBytes(Static.FILE_TABLE_PATH), Bytes.toBytes("/shuwebfs/00000000/" + fileName));
            put.addColumn(Bytes.toBytes(Static.FILE_TABLE_CF), Bytes.toBytes(Static.FILE_TABLE_Auth), Bytes.toBytes("公开"));
            put.addColumn(Bytes.toBytes(Static.FILE_TABLE_CF), Bytes.toBytes(Static.FILE_TABLE_TIME), Bytes.toBytes(System.currentTimeMillis() + ""));
            fileTable.put(put);
            //文件传入hdfs

            FSDataOutputStream out = fs.create(new Path("/shuwebfs/00000000/" + commonsFileInfoVO.getFileName()));
            out.write(decode);

            out.flush();
            out.close();
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        } finally {
            try {
                if (fs != null || hBaseConn != null) {
                    HdfsConnectionPool.releaseConnection(fs);
                    HbaseConnectionPool.releaseConnection(hBaseConn);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return true;
    }


    @PostMapping("searchFile")
    public List SearchFile(@RequestBody ApiSearchFileInfo apiSearchFileInfo) {
        Connection hBaseConn = null;
        Table fileTable = null;
        try {

            hBaseConn = HbaseConnectionPool.getHbaseConnection();
            fileTable = hBaseConn.getTable(TableName.valueOf(Static.FILE_TABLE));
            String uId = apiSearchFileInfo.getUserId();
            List<ApiFileVo> fileInfoVOList = new ArrayList<>();
            Scan scan = new Scan();
            scan.setMaxVersions();
            FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL);
            Filter colFilter = new PrefixFilter(Bytes.toBytes(uId));
            filterList.addFilter(colFilter);
            scan.setFilter(filterList);
            ResultScanner scanner = fileTable.getScanner(scan);
            for (Result result : scanner) {
                ApiFileVo apiFileVo = new ApiFileVo();
                for (Cell cell : result.rawCells()) {
                    apiFileVo.setFileId(Bytes.toString(CellUtil.cloneRow(cell)));
                    String re = Bytes.toString(CellUtil.cloneQualifier(cell));
                    switch (re) {
                        case "name":
                            apiFileVo.setName(Bytes.toString(CellUtil.cloneValue(cell)));
                            break;
                        case "size":
                            if (!Bytes.toString(CellUtil.cloneValue(cell)).equals("-")) {
                                apiFileVo.setSize(Bytes.toLong(CellUtil.cloneValue(cell)));
                            } else {
                                apiFileVo.setSize(null);
                            }
                            break;
                        case "type":
                            apiFileVo.setType(Bytes.toString(CellUtil.cloneValue(cell)));
                            break;
                        case "time":
                            apiFileVo.setTime(Long.parseLong(Bytes.toString(CellUtil.cloneValue(cell))));
                            break;
                        default:
                            break;
                    }
                }
                fileInfoVOList.add(apiFileVo);
            }
            return fileInfoVOList;
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        } finally {
            try {
                if (hBaseConn != null) {
                    HbaseConnectionPool.releaseConnection(hBaseConn);
                }

            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }


    @PostMapping("downLoadFile")
    public boolean downLoadFile(@RequestBody ApiDownLoadInfo apiDownLoadInfo) {
        FileSystem fs = null;
        Connection hBaseConn = null;
        Table fileTable = null;
        if (apiDownLoadInfo.getFileId().isEmpty()
                || apiDownLoadInfo.getDesPath().isEmpty()) {
            return false;
        }
        try {
            if (!apiDownLoadInfo.getUserId().equals(apiDownLoadInfo.getFileId().substring(0, 8))) {
                return false;
            }
            fs = HdfsConnectionPool.getHdfsConnection();
            hBaseConn = HbaseConnectionPool.getHbaseConnection();
            fileTable = hBaseConn.getTable(TableName.valueOf(Static.FILE_TABLE));
            String fileId = apiDownLoadInfo.getFileId();
            String desPath = apiDownLoadInfo.getDesPath();
            //根据fileId在表中获得路径
            Get get = new Get(Bytes.toBytes(fileId));
            get.addColumn(Bytes.toBytes(Static.FILE_TABLE_CF), Bytes.toBytes(Static.FILE_TABLE_PATH));
            Result result = fileTable.get(get);
            if (!result.isEmpty()) {
                Cell cell = result.rawCells()[0];
                String path = Bytes.toString(CellUtil.cloneValue(cell));
                InputStream in = fs.open(new Path(path));
                FileOutputStream out = new FileOutputStream(desPath);
                IOUtils.copyBytes(in, out, 4096, false);

            }
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        } finally {
            try {
                if (fs != null || hBaseConn != null) {
                    HdfsConnectionPool.releaseConnection(fs);
                    HbaseConnectionPool.releaseConnection(hBaseConn);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return true;
    }
}
