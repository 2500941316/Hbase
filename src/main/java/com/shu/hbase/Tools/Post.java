package com.shu.hbase.Tools;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.shu.hbase.Pojo.Api.ApiDownLoadInfo;
import com.shu.hbase.Pojo.Api.ApiSearchFileInfo;
import com.shu.hbase.Pojo.Api.ApiUploadFileInfo;
import com.shu.hbase.Tools.API.Base64Tool;
import com.shu.hbase.Tools.API.Md5;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.HttpURLConnection;
import java.net.URL;

public class Post {
    private static ObjectMapper objectMapper = new ObjectMapper();

    public static void main(String[] args) throws Exception {
        Post();
    }

    public static void Post() throws Exception {

        for (int i = 0; i < 3; i++) {
            ApiUploadFileInfo commonsFileInfoVO = new ApiUploadFileInfo();
            String userId = "00000000"; //公共账号文件夹
            String fileName = "上海大学奖学金评选申请"+i+".docx";
            String time = System.currentTimeMillis() + "";
            String salt = "C6K02DUeJct3VGn7";
            String text = userId + time + salt;
            String key = Md5.md5(text, salt);
            String fileId="19721631_1591844996293";

            ApiSearchFileInfo apiSearchFileInfo=new ApiSearchFileInfo();
            apiSearchFileInfo.setKey(key);
            apiSearchFileInfo.setTime(time);
            apiSearchFileInfo.setUserId("00000000");

            commonsFileInfoVO.setUserId(userId);
            commonsFileInfoVO.setFileName(fileName);
            commonsFileInfoVO.setFileStr(Base64Tool.fileToBase64("/usr/local/springboot/上海大学网盘测试文档.docx"));
            commonsFileInfoVO.setTime(time);
            commonsFileInfoVO.setKey(key);

            ApiDownLoadInfo apiDownLoadInfo=new ApiDownLoadInfo();
            apiDownLoadInfo.setUserId(userId);
            apiDownLoadInfo.setFileId(fileId);
            apiDownLoadInfo.setTime(time);
            apiDownLoadInfo.setKey(key);
            //  apiDownLoadInfo.setDesPath("/usr/local/9999.jpg");
            apiDownLoadInfo.setDesPath("D://HDFSTest.jpg");

            String json = objectMapper.writeValueAsString(commonsFileInfoVO);
            sendPost("http://202.120.117.43:8080/commonAPI/shareToAll", json);
             //sendPost("http://10.10.0.92:8080/commonAPI/shareToAll", json);
            // sendPost("http://localhost:8080/commonAPI/shareToAll", json);
        }
    }

    public static String sendPost(String url, String json) {
        PrintWriter out = null;
        BufferedReader in = null;
        String result = "";
        try {
            URL realUrl = new URL(url);
            // 打开和URL之间的连接
            HttpURLConnection conn = (HttpURLConnection) realUrl.openConnection();
            // 设置通用的请求属性
            conn.setRequestMethod("POST");
            // 发送POST请求必须设置下面的属性
            conn.setDoOutput(true);
            conn.setDoInput(true);
            conn.setUseCaches(false);
            //设置请求属性
            conn.setRequestProperty("Content-Type", "application/json; charset=utf-8");
            conn.connect();
            // 获取URLConnection对象对应的输出流
            out = new PrintWriter(conn.getOutputStream());
            // 发送请求参数
            out.print(json);
            // flush输出流的缓冲
            out.flush();
            // 定义BufferedReader输入流来读取URL的响应
            in = new BufferedReader(new InputStreamReader(conn.getInputStream()));
            String line = "";
            while ((line = in.readLine()) != null) {
                result += line;
            }
            //将返回结果转换为字符串
        } catch (Exception e) {
            throw new RuntimeException("远程通路异常" + e.toString());
        }
        // 使用finally块来关闭输出流、输入流
        finally {
            try {
                if (out != null) {
                    out.close();
                }
                if (in != null) {
                    in.close();
                }
            } catch (IOException ex) {
                ex.printStackTrace();
            }
        }
        System.out.println(result);
        return result;
    }

}
