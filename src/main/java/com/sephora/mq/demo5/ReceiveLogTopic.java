package com.sephora.mq.demo5;


import com.rabbitmq.client.*;

import java.io.IOException;

/*
如何通过多重标准接收消息
使用了topic路由器，可通过灵活的路由键和绑定键的设置，
进一步增强消息选择的灵活性
 */
public class ReceiveLogTopic {
    private static final String EXCHANGE_NAME = "topic_logs";

    public static void main(String[] args) {
        Connection connection = null;
        Channel channel = null;
        try{
            ConnectionFactory factory = new ConnectionFactory();
            factory.setHost("localhost");
            connection = factory.newConnection();
            channel = connection.createChannel();

            channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.TOPIC);
            String queueName = channel.queueDeclare().getQueue();
            System.out.println("[*] Waiting for messages. To exit press CTRL+C");
            String routingKeys[] = {"kern.*", "*.critical"};

            for(String routingKey: routingKeys){
                channel.queueBind(queueName,EXCHANGE_NAME,routingKey);
            }

            Consumer consumer = new DefaultConsumer(channel){
                @Override
                public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                    System.out.println("[x] receive " + envelope.getRoutingKey() + " "+ new String(body,"utf-8"));
                }
            };

            channel.basicConsume(queueName,true,consumer);

        }catch (Exception e){
            e.printStackTrace();
        }

    }
}
