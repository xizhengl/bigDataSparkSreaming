package com.fengli;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;

/**
 * @author Administrator
 */
@SpringBootApplication(exclude={DataSourceAutoConfiguration.class,})
public class TestMavenApplication {

	public static void main(String[] args) {
		SpringApplication.run(TestMavenApplication.class, args);
	}
}
