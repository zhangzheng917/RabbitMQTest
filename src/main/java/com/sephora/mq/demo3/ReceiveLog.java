package com.sephora.mq.demo3;

import com.rabbitmq.client.*;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

/*
如何同一个消息同时发给多个消费者
开始引入RabbitMQ消息模型中的重要概念路由器Exchange以及绑定等
使用了fanout类型的路由器
 */
public class ReceiveLog {

    private static final String EXCHANGE_NAME = "logs";
    public static void main(String[] args) throws IOException, TimeoutException {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();

        channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.FANOUT);
        String queueName = channel.queueDeclare().getQueue();
//        String queueName = "zz";
//        channel.queueDeclare("zz",false,false,false,null);
        //fanout的分发形式是对不同队列的消费者而言的。上面那样使用同一个队列的消费者只会有一个收到消息。

        channel.queueBind(queueName,EXCHANGE_NAME,"");
        System.out.println(" [*] Waiting for messages. To exit press CTRL+C");

        Consumer consumer = new DefaultConsumer(channel){
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                System.out.println("[x] receive " + new String(body,"utf-8"));
            }
        };

        channel.basicConsume(queueName,true,consumer);
    }
}
