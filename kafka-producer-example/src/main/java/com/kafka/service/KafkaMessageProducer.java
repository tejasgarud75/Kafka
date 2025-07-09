package com.kafka.service;

import com.kafka.dto.Customer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.util.concurrent.CompletableFuture;

@Service
public class KafkaMessageProducer {
    @Autowired
    private KafkaTemplate<String, Object> kafkaTemplate;


    @Value("${spring.kafka.topic}")
    private String topic;

    public void sendMessage(String message) {
        CompletableFuture<SendResult<String, Object>> send = kafkaTemplate.send(topic, message);
        send.whenComplete((result, exception) -> {
            if (exception != null) {
                System.err.println("Error sending message: " + exception.getMessage() +
                        " to topic: " + result.getProducerRecord().topic() +
                        " with partition: " + result.getRecordMetadata().partition() +
                        " and offset: " + result.getRecordMetadata().offset());
            } else {
                System.out.println(
                        "Message sent successfully: " + result.getProducerRecord().value() +
                                " to topic: " + result.getProducerRecord().topic() +
                                " with partition: " + result.getRecordMetadata().partition() +
                                " and offset: " + result.getRecordMetadata().offset());

            }
        });
    }

    public void sendCustomer(Customer customer,int no) {
        try {
            CompletableFuture<SendResult<String, Object>> send ;
            if (no==0){
                send = kafkaTemplate.send(topic,customer);
            } else {
                send = kafkaTemplate.send(topic,no,null,customer);
            }
            send.whenComplete(KafkaMessageProducer::accept);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void accept(SendResult<String, Object> result, Throwable exception) {
        if (exception != null) {
            System.err.println("Error sending message: " + exception.getMessage() +
                    " to topic: " + result.getProducerRecord().topic() +
                    " with partition: " + result.getRecordMetadata().partition() +
                    " and offset: " + result.getRecordMetadata().offset());
        } else {
            System.out.println(
                    "Message sent successfully: " + result.getProducerRecord().value() +
                            " to topic: " + result.getProducerRecord().topic() +
                            " with partition: " + result.getRecordMetadata().partition() +
                            " and offset: " + result.getRecordMetadata().offset());

        }
    }
}
