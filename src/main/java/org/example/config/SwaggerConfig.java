package org.example.config;


import dev.mccue.guava.collect.Lists;
import springfox.documentation.builders.ApiInfoBuilder;
import springfox.documentation.service.ApiInfo;
import springfox.documentation.service.ApiKey;
import springfox.documentation.service.AuthorizationScope;
import springfox.documentation.service.SecurityReference;

import java.util.List;

public class SwaggerConfig {

    public ApiInfo apiInfo(String title,String description,String version)
    {
        return new ApiInfoBuilder().title(title)
                .description(description).version(version).build();
    }

    public List<SecurityReference> defaultAuth()
    {
        AuthorizationScope authorizationScope = new AuthorizationScope("global", "accessEverything");
        AuthorizationScope[] authorizationScopes = new AuthorizationScope[1];
        authorizationScopes[0] = authorizationScope;
        return Lists.newArrayList(new SecurityReference("JWT", authorizationScopes));
    }

    public ApiKey apiKey()
    {
        return new ApiKey("JWT", "Authorization", "header");
    }
}

