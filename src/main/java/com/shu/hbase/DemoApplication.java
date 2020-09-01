package com.shu.hbase;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.core.io.ClassPathResource;

import java.io.IOException;

@SpringBootApplication
public class DemoApplication {

    public static void main(String[] args) throws IOException {
        System.setProperty("javax.net.ssl.trustStore",
              "/usr/local/springboot/krb/BDGRootCA.truststore.jks");
        System.setProperty("javax.net.ssl.trustStorePassword", "shu");
        SpringApplication.run(DemoApplication.class, args);
    }

}
