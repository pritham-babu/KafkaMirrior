package org.example.config;

import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import springfox.documentation.swagger2.annotations.EnableSwagger2;

@Configuration
@EnableSwagger2
@EnableConfigurationProperties(SwaggerProperties.class)
@Import({SwaggerHelperConfiguration.class})
public class SwaggerConfiguration
{

    SwaggerProperties swaggerProperties;

    SwaggerConfig swaggerConfig;

    SwaggerConfiguration(SwaggerProperties swaggerProperties,SwaggerConfig swaggerConfig){
        this.swaggerProperties=swaggerProperties;
        this.swaggerConfig = swaggerConfig;
    }

}
