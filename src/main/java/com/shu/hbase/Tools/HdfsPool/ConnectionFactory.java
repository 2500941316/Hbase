package com.shu.hbase.Tools.HdfsPool;

import com.shu.hbase.Tools.HdfsPool.Hdfs.HDfsConn;
import org.apache.commons.pool2.BasePooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.apache.hadoop.fs.FileSystem;

import java.io.IOException;
import java.net.URISyntaxException;

public class ConnectionFactory extends BasePooledObjectFactory<FileSystem> {
    //创建一个新的对象放入池中
    @Override
    public FileSystem create() throws IOException {
        //创建对象
        return HDfsConn.getConnection();
    }

    //用PooledObject封装对象放入池中
    @Override
    public PooledObject<FileSystem> wrap(FileSystem fileSystem) {
        //包装实际对象
        return new DefaultPooledObject<>(fileSystem);
    }
}