package com.sephora.mq.demo2;

import com.rabbitmq.client.*;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
/*
工作队列，即一个生产者对多个消费者
循环分发、消息确认、消息持久、公平分发
 */
public class Worker {

    private static final String queueName = "task_queue";
    public static void main(String[] args) throws IOException, TimeoutException {

        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        Connection connection = factory.newConnection();
        final Channel channel = connection.createChannel();

        channel.queueDeclare(queueName,true,false,false,null);

        System.out.println("[*] Waiting for messages. To exit press CTRL+C");

        //消息确认
        channel.basicQos(1);
        //不要同时发送多个消息给我，每次只发1个，
        // 当我处理完这个消息并给你确认信息后，你再发给我下一个消息。
        // 这时候，RabbitMQ就不会轮流平均发送消息了，而是寻找闲着的工作者。
        Consumer consumer = new DefaultConsumer(channel){
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                String message = new String(body,"utf-8");
                System.out.println("[x] receive " + message);
                try{
                    doWork(message);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } finally {
                    System.out.println("[x] done");
                    channel.basicAck(envelope.getDeliveryTag(),false);//消息确认
                }
            }
        };

        boolean autoAck = false;//关闭默认的消息确认
        channel.basicConsume(queueName, autoAck, consumer);

    }


    private static void doWork(String message) throws InterruptedException {
        for(char ch : message.toCharArray()){
            if(ch == '.') TimeUnit.SECONDS.sleep(1);
        }
    }
}
