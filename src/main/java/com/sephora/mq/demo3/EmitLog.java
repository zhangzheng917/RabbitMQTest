package com.sephora.mq.demo3;

import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

/*
如何同一个消息同时发给多个消费者
开始引入RabbitMQ消息模型中的重要概念路由器Exchange以及绑定等
使用了fanout类型的路由器
 */
public class EmitLog {

    private static final String EXCHANGE_NAME = "logs";
    public static void main(String[] args) throws IOException, TimeoutException {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();

        channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.FANOUT);
        String message = "logs....";

        channel.basicPublish(EXCHANGE_NAME,"",null,message.getBytes("utf-8"));

        System.out.println("[x] send " + message);

        channel.close();
        connection.close();

    }
}
