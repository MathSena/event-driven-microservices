package com.mathsena.orderservice.controller;

import com.mathsena.basedomains.dto.Order;
import com.mathsena.basedomains.dto.OrderEvent;
import com.mathsena.orderservice.kafka.OrderProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.UUID;

@RestController
@RequestMapping("/api/v1")
public class OrderController {

    private static final Logger LOGGER = LoggerFactory.getLogger(OrderController.class);

    private final OrderProducer orderProducer;

    public OrderController(OrderProducer orderProducer) {
        this.orderProducer = orderProducer;
    }

    @PostMapping("/orders")
    public ResponseEntity<String> placeOrder(@RequestBody Order order){
        order.setOrderId(UUID.randomUUID().toString());

        OrderEvent orderEvent = new OrderEvent();
        orderEvent.setStatus("PENDING");
        orderEvent.setMessage("order status is pending...");
        orderEvent.setOrder(order);

        try {
            orderProducer.sendMessage(orderEvent);
            LOGGER.info("Order placed successfully: {}", orderEvent);
            return ResponseEntity.ok("Order placed successfully");
        } catch (Exception e) {
            LOGGER.error("Error placing order: {}", orderEvent, e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body("Error placing order, please try again later");
        }
    }
}
