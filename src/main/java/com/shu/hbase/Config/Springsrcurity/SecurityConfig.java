
package com.shu.hbase.Config.Springsrcurity;

import com.shu.hbase.Config.Springsrcurity.ShuLogin.ShuFilter;
import com.shu.hbase.Config.Springsrcurity.TokenLogin.TokenAuthenticationFilter;
import com.shu.hbase.Config.Springsrcurity.TokenLogin.TokenAuthenticationSuccessHandler;
import com.shu.hbase.Config.Springsrcurity.TokenLogin.TokenAuthticationProvider;
import com.shu.hbase.Config.Springsrcurity.TokenLogin.TokenLoginFilter;
import org.apache.hadoop.security.http.CrossOriginFilter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.config.annotation.authentication.builders.AuthenticationManagerBuilder;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.security.config.annotation.web.configuration.WebSecurityConfigurerAdapter;
import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder;
import org.springframework.security.web.authentication.UsernamePasswordAuthenticationFilter;
import org.springframework.security.web.csrf.CsrfFilter;


@Configuration
@EnableWebSecurity
public class SecurityConfig extends WebSecurityConfigurerAdapter {
    @Autowired
    private ShuFilter shuFilter;

    @Autowired
    private TokenLoginFilter tokenFilter;
    @Autowired
    private MyAuthenticationSuccessHandler authenticationSuccessHandler;

    @Autowired
    private TokenAuthenticationSuccessHandler tokenAuthenticationSuccessHandler;

    @Autowired
    private MyAuthenticationFailHandler authenticationFailHandler;

    @Autowired
    private MyUserDetailService myUserDetailService;

    @Autowired
    public void configGlobal(AuthenticationManagerBuilder auth)  {
        auth.authenticationProvider(customAuthenticationProvider()).eraseCredentials(true);
    }

    //用户名和密码登陆处理
    @Bean
    public TokenAuthticationProvider customAuthenticationProvider() {
        TokenAuthticationProvider tokenAuthticationProvider=new TokenAuthticationProvider();
        return tokenAuthticationProvider;
    }


/**
     * 添加token登陆验证的过滤器
     */

    @Bean
    public TokenAuthenticationFilter tokenAuthenticationFilter() throws Exception {
        TokenAuthenticationFilter filter = new TokenAuthenticationFilter();
        filter.setAuthenticationManager(authenticationManager());
        filter.setAuthenticationSuccessHandler(tokenAuthenticationSuccessHandler);
        filter.setAuthenticationFailureHandler(authenticationFailHandler);
        return filter;
    }

    @Override
    protected void configure(HttpSecurity http) throws Exception {

        http
                .addFilterBefore(shuFilter, CsrfFilter.class)
                .addFilterBefore(tokenFilter, UsernamePasswordAuthenticationFilter.class)
                .addFilterBefore(tokenAuthenticationFilter(), UsernamePasswordAuthenticationFilter.class)

                .cors().and().csrf().disable()
                .exceptionHandling().authenticationEntryPoint(new EntryPoint()).and()
                .requestMatchers()
                .antMatchers("/*")
                .and()

                .authorizeRequests()
                .antMatchers("/").permitAll()
                .antMatchers("/favicon.ico").permitAll()
                .antMatchers("/uploadToBacken").permitAll()
                .anyRequest().authenticated()
                .and()
                .formLogin().permitAll()
                .successHandler(authenticationSuccessHandler)
                .failureHandler(authenticationFailHandler)
                .and().rememberMe()
                .tokenValiditySeconds(60 * 60 * 24)
                .userDetailsService(myUserDetailService)

                .and().logout().logoutSuccessHandler(new MyLogoutSuccessHandler()).deleteCookies("JSESSIONID");
    }

    public void configure(AuthenticationManagerBuilder auth) throws Exception {
        auth.userDetailsService(myUserDetailService).passwordEncoder(new BCryptPasswordEncoder());
        auth.authenticationProvider(customAuthenticationProvider());
    }
}

