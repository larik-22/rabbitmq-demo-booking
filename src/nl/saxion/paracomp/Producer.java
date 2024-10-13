package nl.saxion.paracomp;

import com.rabbitmq.client.*;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Scanner;
import java.util.concurrent.TimeoutException;

public class Producer {
    private Channel channel;

    public static void main(String[] args) throws IOException, TimeoutException {
        new Producer().run();
    }

    public void run() throws IOException, TimeoutException {
        //todo: open a channel to your RabbitMQ instance and send messages to the default exchange
        ConnectionFactory factory = new ConnectionFactory();

        try {
            Connection connection = factory.newConnection();
            channel = connection.createChannel();
            startProducing();
            //todo: close you channel
            channel.close();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    private void startProducing() throws IOException {
        Scanner scanner = new Scanner(System.in);
        System.out.println("Enter q to quit the application.");
        System.out.println("Which message should I send?");
        String message = scanner.nextLine();
        while (!message.equals("q")) {
            //todo: send your message here.
            channel.basicPublish("", "hello", null, message.getBytes(StandardCharsets.UTF_8));
            System.out.println("    [x] Sent '" + message + "'");
            System.out.println("Which message should I send?");
            message = scanner.nextLine();
        }

    }


}
