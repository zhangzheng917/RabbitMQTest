package com.sephora.mq.demo5;

import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.io.IOException;

/*
如何通过多重标准接收消息
使用了topic路由器，可通过灵活的路由键和绑定键的设置，
进一步增强消息选择的灵活性
 */
public class EmitLogTopic {
    private static final String EXCHANGE_NAME = "topic_logs";

    public static void main(String[] args) {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");

        Connection connection = null;
        Channel channel = null;

        try{
            connection = factory.newConnection();
            channel = connection.createChannel();
            channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.TOPIC);

            String routingKey = "cron.warn";
            String message = "A cron.warn";

            channel.basicPublish(EXCHANGE_NAME,routingKey,null,message.getBytes("utf-8"));
            System.out.println(" [x] Sent '" + routingKey + "':'" + message + "'");

        }catch ( Exception e){
            e.printStackTrace();
        }finally {
            if(channel != null){
                try{
                    channel.close();
                }catch (Exception e){
                    e.printStackTrace();
                }
            }
            if(connection != null){
                try{
                    connection.close();
                }catch (Exception e){
                    e.printStackTrace();
                }
            }
        }

    }

}
