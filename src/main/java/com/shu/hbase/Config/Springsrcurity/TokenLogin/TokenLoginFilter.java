package com.shu.hbase.Config.Springsrcurity.TokenLogin;


import com.shu.hbase.Config.Springsrcurity.MyAuthenticationFailHandler;
import com.shu.hbase.Tools.API.Md5;
import org.springframework.security.core.AuthenticationException;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;

import javax.servlet.FilterChain;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.Map;

@Component
public class TokenLoginFilter extends OncePerRequestFilter {
    private MyAuthenticationFailHandler myAuthenticationFaiureHandler = new MyAuthenticationFailHandler();

    public MyAuthenticationFailHandler getMyAuthenticationFaiureHandler() {
        return myAuthenticationFaiureHandler;
    }

    public void setMyAuthenticationFaiureHandler(MyAuthenticationFailHandler myAuthenticationFaiureHandler) {
        this.myAuthenticationFaiureHandler = myAuthenticationFaiureHandler;
    }

    @Override
    protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response, FilterChain filterChain) throws ServletException, IOException {
        ServletRequest requestWrapper = null;
        if (request.getRequestURI().contains("commonAPI")) {
            try {

                requestWrapper = new MAPIHttpServletRequestWrapper(request);
                Map<String,Object> resultMap = MAPIHttpServletRequestWrapper.getBodyMap(requestWrapper.getInputStream());
                if(resultMap!=null&&!resultMap.isEmpty()&&resultMap.get("userId")!=null) {
                    String userId = (String) resultMap.get("userId");
                    String key = (String) resultMap.get("key");
                    String time = (String) resultMap.get("time");
                    String salt = "C6K02DUeJct3VGn7";
                    String text=userId+time+salt;
                    if (!Md5.verify(text, salt, key)) {
                        throw new TokenException("验证失败");
                    }
                    filterChain.doFilter(requestWrapper, response);
                    return;
                }
            } catch (AuthenticationException e) {
                myAuthenticationFaiureHandler.onAuthenticationFailure(request, response, e);
                return;
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        filterChain.doFilter(request, response);

    }
}

