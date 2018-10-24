package com.sephora.mq.demo6_rpc;

import com.rabbitmq.client.*;

import java.io.IOException;
/*
①如果没有服务端在运行，客户端该怎么办
②客户端应该为一次RPC设置超时吗
③如果服务端发生故障并抛出异常，它还应该返回给客户端吗？
④在处理消息前，先通过边界检查、类型判断等手段过滤掉无效的消息等
 */
public class RPCServer {

    private static final String RPC_QUEUE_NAME = "rpc_queue";

    //模拟的耗时任务，即计算斐波那契数
    private static int fib(int n){
        if(n==0) return 0;
        if(n==1) return 1;
        return fib(n-1) + fib(n-2);
    }

    public static void main(String[] args) {
        //创建连接和通道
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");

        Connection connection = null;
        try{
            connection = factory.newConnection();
            final Channel channel = connection.createChannel();

            //声明队列
            channel.queueDeclare(RPC_QUEUE_NAME,false,false,false,null);
            //一次只从队列中取出一个消息
            channel.basicQos(1);

            System.out.println(" [x] Awaiting RPC requests");

            //监听消息（即RPC请求）
            Consumer consumer = new DefaultConsumer(channel){
                @Override
                public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                    AMQP.BasicProperties replyPros = new AMQP.BasicProperties().
                            builder().correlationId(properties.getCorrelationId()).build();
                    System.out.println(properties.getReplyTo());

                    String response = "";
                    try{
                        //收到RPC请求后开始处理
                        String message = new String(body,"utf-8");
                        System.out.println(" [.] fib(" + message + ")");
                        int n = Integer.parseInt(message);
                        response += fib(n);
                    }catch (RuntimeException e){
                        e.printStackTrace();
                    }finally {
                        //处理完之后，返回响应（即发布消息）
                        System.out.println("[server current time] : " + System.currentTimeMillis());
                        channel.basicPublish("",properties.getReplyTo(),replyPros,response.getBytes("utf-8"));
                    }
                }
            };
            channel.basicConsume(RPC_QUEUE_NAME,true,consumer);
        }catch (Exception e){
            e.printStackTrace();
        }

    }
}
