package com.kafka.service;

import com.kafka.dto.Customer;
import org.springframework.kafka.annotation.DltHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.kafka.annotation.TopicPartition;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class KafkaMessageListener {

   /* @KafkaListener(topics = "spring-topic", groupId = "consumer-group-demo")
    public void consume1(String message) {

        System.out.println("Consumer 1 consume message : " + message);

    }

      @KafkaListener(topics = "spring-topic", groupId = "consumer-group-demo")
    public void consume2(String message) {

        System.out.println("Consumer 2 consume message : " + message);

    }

    @KafkaListener(topics = "spring-topic", groupId = "consumer-group-demo")
    public void consume3(String message) {

        System.out.println("Consumer 3 consume message : " + message);

    }

    @KafkaListener(topics = "spring-topic", groupId = "consumer-group-demo")
    public void consume4(String message) {

        System.out.println("Consumer 4 consume message : " + message);

    }*/

    @RetryableTopic(attempts = "4")
    @KafkaListener(topics = "dlt-topic", groupId = "dlt-consumer-customer-group")
    public void consumeCustomerDLT(Customer customer) {
        try {
            List<Integer> problemIds = List.of(9, 8, 7);
            if (problemIds.contains(customer.getId())) {
                throw new RuntimeException("Invalid customer ID: " + customer.getId());
            }
            System.out.println("Successfully processed customer: " + customer);
        } catch (Exception e) {
            throw e;
        }
    }

    // ❷ DLT handler – called only after all retries fail
    @DltHandler
    public void handleCustomerOnDlt(@Payload Customer customer,
                                    @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
                                    @Header(KafkaHeaders.OFFSET) long offset,
                                    @Header(KafkaHeaders.EXCEPTION_MESSAGE) String error) {
        System.err.printf("DLT Handler - topic=%s offset=%d payload=%s error=%s%n", topic, offset, customer, error);
    }


    // Listen from particular partition
    @KafkaListener(topics = "tejas-topic",
            groupId = "consumer-customer-group-demo",
            topicPartitions = {
                    @TopicPartition(topic = "tejas-topic", partitions = {"2"})
            })
    public void consumeCustomer(Customer customer) {

        System.out.println("Consumer consume from 2nd partition : " + customer.toString());

    }

    @KafkaListener(topics = "tejas-topic",
            groupId = "consumer-customer-group-demo",
            topicPartitions = {
                    @TopicPartition(topic = "tejas-topic", partitions = {"3"})
            })
    public void consumeCustomer1(Customer customer) {

        System.out.println("Consumer consume from 3 rd partition : " + customer.toString());

    }


}

