package com.shu.hbase.Config.Springsrcurity;

import com.shu.hbase.Config.Springsrcurity.ShuLogin.ShuFilter;
import org.springframework.security.core.AuthenticationException;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.core.userdetails.User;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.security.core.userdetails.UserDetailsService;
import org.springframework.security.core.userdetails.UsernameNotFoundException;
import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

@Component
public class MyUserDetailService implements UserDetailsService {
    @Override
    public UserDetails loadUserByUsername(String username)  {
        if (username.length() == 8) {
            String auth = "ROLE_USER";
            String password = new BCryptPasswordEncoder().encode(ShuFilter.password);
            List<GrantedAuthority> grantedAuthorities = new ArrayList<GrantedAuthority>();
            grantedAuthorities.add(new SimpleGrantedAuthority(auth));
            return new User(username, password, grantedAuthorities);
        } else {
            throw new UsernameNotFoundException("登录失败");
        }
    }
}

