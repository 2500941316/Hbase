package com.shu.hbase.Config.Springsrcurity.ShuLogin;

import com.fasterxml.jackson.databind.ObjectMapper;

import com.shu.hbase.Pojo.User;
import com.shu.hbase.Tools.SHUTool;
import com.shu.hbase.Tools.TableModel;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;

import javax.servlet.FilterChain;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

@Component
public class ShuFilter extends OncePerRequestFilter {
    public static String password="";
    public static User user=new User();

    @Override
    protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response, FilterChain filterChain) throws ServletException, IOException {
        if (request.getRequestURI().contains("/login"))
        {
            ObjectMapper objectMapper=new ObjectMapper();
            TableModel tableModel=new TableModel();
            String username=request.getParameter("username").trim();
            password=request.getParameter("password").trim();
            if (StringUtils.isNotEmpty(username) || StringUtils.isNotEmpty(password))
            {
                if (SHUTool.getAuth(username,password))
                {
                    if ((user=SHUTool.getInfo(username))!=null)
                    {

                    }
                }else
                {
                    tableModel.error("输入不正确");
                    tableModel.setCode(409);
                    response.setContentType("text/json;charset=utf-8");
                    response.getWriter().write(objectMapper.writeValueAsString(tableModel));
                    return;
                }
            }else
            {

                tableModel.error("用户名或密码不能为空");
                tableModel.setCode(409);
                response.setContentType("text/json;charset=utf-8");
                response.getWriter().write(objectMapper.writeValueAsString(tableModel));
                return;
            }

        }
        filterChain.doFilter(request,response);
    }
}
