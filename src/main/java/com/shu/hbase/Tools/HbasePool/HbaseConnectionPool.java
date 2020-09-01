package com.shu.hbase.Tools.HbasePool;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.apache.hadoop.hbase.client.Connection;


public class HbaseConnectionPool {
    private static ConnectionFactory factory;
    private static ConnectionPoolConfig connectionPool;
    private static GenericObjectPoolConfig config;

    static {
        factory = new ConnectionFactory();
        config = new GenericObjectPoolConfig();
        config.setMaxTotal(30);
        //最小空闲数
        config.setMinIdle(2);
        config.setTimeBetweenEvictionRunsMillis(60000);
        //最大空闲数
        config.setMaxIdle(8);
        config.setMaxWaitMillis(10000);
        connectionPool = new ConnectionPoolConfig(factory, config);
    }

    public static Connection getHbaseConnection() throws Exception {
        try {
            // System.out.println("归还了"+connectionPool.getReturnedCount());
           // System.out.println("借出了"+connectionPool.getBorrowedCount());
            return  connectionPool.borrowObject();
        } catch (Exception e) {
            throw new Exception(e.getMessage());
        }
    }

    public static void releaseConnection(Connection connection) {
        try{
            //System.out.println("归还了连接");
        }catch (Exception e)
        {
            e.printStackTrace();
        }finally {
            connectionPool.returnObject(connection);
        }
    }
}
