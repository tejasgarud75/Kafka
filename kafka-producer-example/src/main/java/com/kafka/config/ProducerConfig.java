package com.kafka.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class ProducerConfig {

    // This method will create a topic automatically
    @Bean
    public NewTopic createNewTopic(){
        return new NewTopic("dlt-topic", 3 , (short) 1);
    }
}
