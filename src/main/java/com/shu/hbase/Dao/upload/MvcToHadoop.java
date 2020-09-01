package com.shu.hbase.Dao.upload;

import com.shu.hbase.Dao.HBaseDao;
import com.shu.hbase.Pojo.Static;
import com.shu.hbase.Tools.FileType.FileType;
import com.shu.hbase.Tools.FileType.FileTypeJudge;
import com.shu.hbase.Tools.HbasePool.HbaseConnectionPool;
import com.shu.hbase.Tools.HdfsPool.HdfsConnectionPool;
import com.shu.hbase.Tools.TableModel;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.io.IOUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;

public class MvcToHadoop {

    public static TableModel createFile(String mvcPath, String backId, String fileId, String uId) throws Exception {
        FileSystem fs = null;
        Connection hBaseConn = null;
        Table userTable = null;
        InputStream typeIn = null;
        InputStream in = null;
        String hdfsPath = null;
        File localPath=null;
        try {
            fs = HdfsConnectionPool.getHdfsConnection();
            hBaseConn = HbaseConnectionPool.getHbaseConnection();
            userTable = hBaseConn.getTable(TableName.valueOf(Static.USER_TABLE));
            //权限验证：上传只能自己给自己上传，backId的前8位必须等于uId
            if (!backId.substring(0, 8).equals(uId)) {
                return TableModel.error("您的权限不足");
            }
             localPath=new File(mvcPath);
            if (!HBaseDao.insertOrUpdateUser(userTable,  localPath.length()+"", uId, "upload")) {
                return TableModel.error("文件超出存储容量");
            }

            in = new FileInputStream(localPath);
            typeIn = new FileInputStream(localPath);
            FileType type = FileTypeJudge.getType(typeIn);
            String fileType = FileTypeJudge.isFileType(type);
            //做二次判断，如果还是"其他",则对文件后缀再次判断
            String[] videoTypes=new String[]{"avi","ram","rm","mpg","mov","asf","mp4","flv","mid"};
            String[] audioTypes=new String[]{"wav","mp3"};
            if (fileType.equals("other"))
            {
                int i = localPath.getName().lastIndexOf(".");

                for (String videoType : videoTypes) {
                    if (videoType.equals(localPath.getName().substring(i+1)))
                    {
                        fileType="video";
                    }else if (audioTypes.equals(localPath.getName().substring(i+1)))
                    {
                        fileType="audio";
                    }
                }
            }
            //查询hdfs的路径
            hdfsPath = HBaseDao.findUploadPath(backId);
            if (hdfsPath != null) {
                hdfsPath = hdfsPath + "/" + localPath.getName();
            }
            HBaseDao.insertToFiles(localPath, fileType, hdfsPath, backId, uId, fileId);
            FSDataOutputStream out = fs.create(new Path(hdfsPath));
            IOUtils.copyBytes(in, out, 4096, true);
            out.close();
            return TableModel.success("上传成功");
        } catch (Exception e) {
            e.printStackTrace();
            return TableModel.error("上传失败");
        } finally {
            localPath.delete();
            userTable.close();
            HbaseConnectionPool.releaseConnection(hBaseConn);
            HdfsConnectionPool.releaseConnection(fs);
        }
    }
}
