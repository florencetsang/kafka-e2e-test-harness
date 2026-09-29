package com.example.app2;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;

@SpringBootApplication
@EnableConfigurationProperties(App2Properties.class)
public class App2Application {

    public static void main(String[] args) {
        SpringApplication.run(App2Application.class, args);
    }
}
