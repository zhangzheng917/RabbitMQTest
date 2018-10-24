package com.sephora.mq.demo4;

import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

/*
如何选择性地接收消息
使用了direct路由器
 */
public class EmitLogDirect {

    private static final String EXCHANGE_NAME = "direct_logs";
    private static final String SEVERITY = "warning";
    public static void main(String[] args) throws Exception{
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();

        channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.DIRECT);

        String message = "msg...";

        channel.basicPublish(EXCHANGE_NAME,SEVERITY,null,message.getBytes("utf-8"));
        System.out.println("[x] sent " + SEVERITY + ": " + message);
        channel.close();
        connection.close();

    }
}
