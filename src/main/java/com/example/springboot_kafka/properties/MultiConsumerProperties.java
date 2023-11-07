package com.example.springboot_kafka.properties;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

// @ConfigurationProperties(prefix = MultiConsumerProperties.PREFIX)
@Data
@AllArgsConstructor
@NoArgsConstructor
public class MultiConsumerProperties {

    public static final String PREFIX = "fancywu.kafka.multi";

    // @Autowired(required = false)
    private boolean enable = false;
}
