package com.shu.hbase.Config.Springsrcurity.TokenLogin;


import org.springframework.security.core.AuthenticationException;

public class TokenException extends AuthenticationException {
    public TokenException(String msg) {
        super(msg);
    }
}
