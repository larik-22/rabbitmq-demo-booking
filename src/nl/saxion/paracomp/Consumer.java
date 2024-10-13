package nl.saxion.paracomp;

import java.io.IOException;
import java.util.Scanner;
import java.util.concurrent.TimeoutException;

import com.rabbitmq.client.*;

public class Consumer implements Runnable {
    private Channel channel;
    private static String name = "DEFAULT_NAME";

    public Consumer(String name) {
        Consumer.name = name;
    }

    @Override
    public void run() {
        ConnectionFactory factory = new ConnectionFactory();

        try {
            Connection connection = factory.newConnection();
            channel = connection.createChannel();
            channel.queueDeclare("hello", false, false, false, null);

            DefaultConsumer consumer = new DefaultConsumer(channel) {
                @Override
                public void handleDelivery(String consumerTag,
                                           Envelope envelope,
                                           AMQP.BasicProperties properties,
                                           byte[] body) throws IOException {
                    String data = new String(body, "UTF-8");
                    System.out.println("Consumer " + name + " received message: " + data);
                }
            };
            channel.basicConsume("hello", true, consumer);

        } catch (Exception e) {
            e.printStackTrace();
        }
        // The code below makes sure we can exit the program when we want
        Scanner scanner = new Scanner(System.in);
        String command = "";
        while (!command.equals("q")) {
            System.out.println("Enter q to quit the application.");
            command = scanner.nextLine();
        }

        try {
            channel.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        } catch (TimeoutException e) {
            throw new RuntimeException(e);
        }
    }
}
