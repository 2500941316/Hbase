package com.shu.hbase.Config.Springsrcurity;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.shu.hbase.Tools.TableModel;
import org.springframework.security.core.AuthenticationException;
import org.springframework.security.web.AuthenticationEntryPoint;
import org.springframework.stereotype.Component;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

@Component
public class EntryPoint implements AuthenticationEntryPoint {
    private ObjectMapper objectMapper = new com.fasterxml.jackson.databind.ObjectMapper();
    public void commence(HttpServletRequest request, HttpServletResponse response,
                         AuthenticationException authException) throws IOException {
        TableModel tableModel = new TableModel();
        tableModel.setCode(405);
        String json = objectMapper.writeValueAsString(tableModel);
        response.setContentType("text/json;charset=utf-8");
        //response.setStatus(405);
        response.getWriter().write(json);
    }
}
