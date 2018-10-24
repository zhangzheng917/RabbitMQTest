package com.sephora.mq.demo6_rpc;

import com.rabbitmq.client.*;

import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeoutException;
/*
①如果没有服务端在运行，客户端该怎么办
②客户端应该为一次RPC设置超时吗
③如果服务端发生故障并抛出异常，它还应该返回给客户端吗？
④在处理消息前，先通过边界检查、类型判断等手段过滤掉无效的消息等
 */
public class RPCClient {
    private Channel channel;
    private  String requestQueueName = "rpc_queue";
    private Connection connection;
    private String replyQueueName;

    //定义一个RPC客户端
    public RPCClient() throws IOException, TimeoutException {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        connection = factory.newConnection();
        channel = connection.createChannel();

        replyQueueName = channel.queueDeclare().getQueue();
    }

    //关闭连接
    public void close() throws IOException {
        connection.close();
    }

    //真正地请求
    public String call(String message) throws IOException, InterruptedException {
        final String uuid = UUID.randomUUID().toString();
        AMQP.BasicProperties props = new AMQP.BasicProperties().builder().correlationId(uuid).replyTo(replyQueueName).build();

        channel.basicPublish("",requestQueueName,props,message.getBytes("utf-8"));
        final BlockingQueue<String> blockingQueue = new ArrayBlockingQueue<String>(1);

        channel.basicConsume(replyQueueName,true,new DefaultConsumer(channel){
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                if(properties.getCorrelationId().equals(uuid)){
                    System.out.println(envelope.getRoutingKey());
                    System.out.println(replyQueueName);
                    System.out.println("[client current time] : " + System.currentTimeMillis());
                    blockingQueue.offer(new String(body,"utf-8"));
                }
            }
        });
        return blockingQueue.take();
    }

    public static void main(String[] args) {
        RPCClient fibonacciRpc = null;
        String response = null;
        try{
            fibonacciRpc = new RPCClient();
            System.out.println(" [x] Requesting fib(30)");
            //RPC客户端发送调用请求，并等待影响，直到接收到
            response = fibonacciRpc.call("30");
            System.out.println(" [.] Got '" + response + "'");
        } catch (TimeoutException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            if(fibonacciRpc!=null){
                try {
                    //关闭RPC客户的连接
                    fibonacciRpc.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
