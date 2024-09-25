package org.example.config;

import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class SwaggerHelperConfiguration {

    @Bean
    @ConditionalOnMissingBean(SwaggerConfig.class)
    SwaggerConfig swaggerConfig()
    {
        return new SwaggerConfig();
    }
}

