package com.sephora.mq.demo4;

import com.rabbitmq.client.*;

import java.io.IOException;

/*
如何选择性地接收消息
使用了direct路由器
 */
public class ReceiveLogDirect {
    private static final String EXCHANGE_NAME = "direct_logs";

    public static void main(String[] args) throws Exception{
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();

        channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.DIRECT);
        String queueName = channel.queueDeclare().getQueue();

        final String [] severity = {"info","warning","error"};

        for(String s : severity){
            channel.queueBind(queueName,EXCHANGE_NAME,s);
        }

        System.out.println("[*] Waiting for messages. To exit press CTRL+C");
        Consumer consumer = new DefaultConsumer(channel){
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                System.out.println("[x] receive " + envelope.getRoutingKey() +": " + new String(body,"utf-8"));
            }
        };

        channel.basicConsume(queueName,true,consumer);
    }
}
