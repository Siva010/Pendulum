package io.pendulum.server;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;

/**
 * Spring's entire job in this project: dependency injection, configuration binding, an HTTP
 * server, and Actuator. The engine in {@code pendulum-core} does not import a single Spring type,
 * which is what lets it be unit-tested without an application context and embedded in something
 * that is not a Boot app.
 */
@SpringBootApplication
@EnableConfigurationProperties(PendulumProperties.class)
public class PendulumServerApplication {

    public static void main(String[] args) {
        SpringApplication.run(PendulumServerApplication.class, args);
    }
}
