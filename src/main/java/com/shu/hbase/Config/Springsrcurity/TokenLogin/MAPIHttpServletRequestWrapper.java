package com.shu.hbase.Config.Springsrcurity.TokenLogin;
import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

import javax.servlet.ReadListener;
import javax.servlet.ServletInputStream;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletRequestWrapper;

import com.alibaba.fastjson.JSONObject;
import com.shu.hbase.Tools.API.JsonUtil;

public class MAPIHttpServletRequestWrapper extends HttpServletRequestWrapper{
    private final byte[] body; // 报文

    public MAPIHttpServletRequestWrapper(HttpServletRequest request) throws IOException {
        super(request);
        InputStream is = request.getInputStream();
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        byte buff[] = new byte[ 1024 ];
        int read;
        while( ( read = is.read( buff ) ) > 0 ) {
            baos.write( buff, 0, read );
        }
        body = baos.toByteArray();

    }

    @Override
    public BufferedReader getReader() throws IOException {
        return new BufferedReader(new InputStreamReader(getInputStream()));
    }

    @Override
    public ServletInputStream getInputStream() throws IOException {
        final ByteArrayInputStream bais = new ByteArrayInputStream(body);
        return new ServletInputStream() {

            @Override
            public boolean isFinished() {
                return false;
            }

            @Override
            public boolean isReady() {
                return false;
            }

            @Override
            public void setReadListener(ReadListener readListener) {

            }

            @Override
            public int read() throws IOException {
                return bais.read();
            }
        };
    }

    //获取request请求body中参数
    public static Map<String,Object> getBodyMap(InputStream in) {
        String param= null;
        BufferedReader streamReader=null;
        try {
            streamReader = new BufferedReader( new InputStreamReader(in, "UTF-8"));
            StringBuilder responseStrBuilder = new StringBuilder();
            String inputStr;
            while ((inputStr = streamReader.readLine()) != null)
                responseStrBuilder.append(inputStr);
            if(!JsonUtil.getInstance().validate(responseStrBuilder.toString())){
                return new HashMap<String, Object>();
            }
            JSONObject jsonObject = JSONObject.parseObject(responseStrBuilder.toString());
            if(jsonObject==null){
                return new HashMap<String, Object>();
            }
            param= jsonObject.toJSONString();


        } catch (Exception e) {
            e.printStackTrace();
        }finally{
            if(streamReader!=null){
                try {
                    streamReader.close();
                } catch (IOException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            }

        }
        return JSONObject.parseObject(param,Map.class);
    }
}
