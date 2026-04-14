package com.example.timedeal;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.data.jpa.repository.config.EnableJpaAuditing;

@SpringBootApplication
@EnableJpaAuditing
public class TimeDealServiceApplication {

    public static void main(String[] args) {
        SpringApplication.run(TimeDealServiceApplication.class, args);
    }

}
