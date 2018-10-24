package com.sephora.mq.demo2;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.MessageProperties;

import java.io.IOException;
import java.util.concurrent.TimeoutException;
/*
工作队列，即一个生产者对多个消费者
循环分发、消息确认、消息持久、公平分发
 */
public class NewTask {

    private static final String queueName = "task_queue";//RabbitMQ 不允许重新定义存在的队列,所以要改队列名字

    public static void main(String[] args) throws IOException, TimeoutException {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");

        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();


        String message = "6......";
        //队列持久化
        boolean durable = true;
        channel.queueDeclare(queueName,durable,false,false,null);
        //MessageProperties.PERSISTENT_TEXT_PLAIN 消息持久化
        channel.basicPublish("",queueName, MessageProperties.PERSISTENT_TEXT_PLAIN,message.getBytes("utf-8"));

        System.out.println("[x] sent " + message);

        channel.close();
        connection.close();
    }
}
