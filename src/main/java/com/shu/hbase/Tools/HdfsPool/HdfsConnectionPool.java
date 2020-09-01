package com.shu.hbase.Tools.HdfsPool;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.apache.hadoop.fs.FileSystem;


public class HdfsConnectionPool {
    private static ConnectionFactory factory;
    private static ConnectionPoolConfig connectionPool;
    private static GenericObjectPoolConfig config;

    static {
        factory = new ConnectionFactory();
        config = new GenericObjectPoolConfig();
        config.setMaxTotal(20);
        //最小空闲数
        config.setMinIdle(2);
        //最大空闲数
        config.setMaxIdle(8);
        config.setMaxWaitMillis(10000);
        connectionPool = new ConnectionPoolConfig(factory, config);
    }

    public static FileSystem getHdfsConnection() throws Exception {
        try {
           // System.out.println("归还了"+connectionPool.getReturnedCount());
            //System.out.println("借出了"+connectionPool.getBorrowedCount());
            return  connectionPool.borrowObject();
        } catch (Exception e) {
            throw new Exception(e.getMessage());
        } finally {
        }
    }

    public static void releaseConnection(FileSystem fileSystem) throws Exception {
        try {
        } catch (Exception e) {
            throw new Exception(e.getMessage());
        } finally {
                connectionPool.returnObject(fileSystem);
        }
    }
}
