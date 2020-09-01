package com.shu.hbase.Dao.download;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.FileSystem;
import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

public class DownLoad {
    public static void downloadFromHDFSinOffset(FileSystem fs, HttpServletResponse response, String encryptfilename, HttpServletRequest request) throws IOException {

        if (response == null || encryptfilename == null || encryptfilename.equals(""))
            return;
        final String userAgent = request.getHeader("USER-AGENT");
        //判断浏览器代理并分别设置响应给浏览器的编码格式
        String finalFileName = null;
        if (StringUtils.contains(userAgent, "Mozilla")) {//google,火狐浏览器
            finalFileName = new String(encryptfilename.substring(encryptfilename.lastIndexOf("/") + 1).getBytes(), "ISO8859-1");
        } else {
            //finalFileName = URLEncoder.encode(encryptfilename.substring(encryptfilename.lastIndexOf("/") + 1), "UTF8");//其他浏览器
            finalFileName = encryptfilename.substring(encryptfilename.lastIndexOf("/") + 1);
        }
        response.setContentType("application/x-msdownload");
        response.addHeader("Content-Disposition", "attachment;filename=" + finalFileName);
        ServletOutputStream sos = response.getOutputStream();

        DownloadInOffset dfb = null;
        try {
            //判断fs中是否存在该文件的目录，如果存在并且大小不等于session的大小，则开始读出offset，否则offset等于0
            //将out放在session中
            long offSet = 0;
            dfb = new DownloadInOffset(fs, encryptfilename);
            byte[] buffer = new byte[1024];
            long size = dfb.getFileSize(fs, encryptfilename);// 文件总大小
            response.setHeader("Content-Length", size + "");
            //System.out.println("HDFSHandler : 文件总大小 = " + size);
            int len = 0;// 每次读取字节长度
            long length = 0;// 已读取总长度
            if (offSet == 0) {
                len = dfb.download(buffer);// 将指针指向文件起始处
            } else {
                len = dfb.download(buffer, offSet);// 先将指针指向偏移量位置
            }
            do {
                // 开始循环，往buffer中写入输出流
                sos.write(buffer, 0, len);
                length += len;
                //System.out.println(length);
                //将length写入
            } while ((len = dfb.download(buffer)) != -1 && length + offSet <= size);
            //System.out.println("HDFSHandler : 开始下载的开始点 = " + offSet);
            // System.out.println("HDFSHandler : 本次上传的大小 = " + length);
            // System.out.println("HDFSHandler : offset + length = " + offSet + "+" + length + "=" + (offSet + length));
            sos.flush();
        } catch (Exception e) {

        } finally {
            dfb.close();
        }
    }
}