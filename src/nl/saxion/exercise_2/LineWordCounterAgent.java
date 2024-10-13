package nl.saxion.exercise_2;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;

import java.io.IOException;

/**
 * This agent listens to messages coming from a RabbitMQ server and counts
 * the number of words in the message.
 */
public class LineWordCounterAgent {

    private Channel channel;

    public static void main(String[] args) {
        new LineWordCounterAgent().listenForLineCountTasks();
    }


    /**
     * Listen for messages from RabbitMQ
     * Parse the message and count the number of words.
     * Send a message containing the number of words in the line.
     */
    private void listenForLineCountTasks() {
        ConnectionFactory factory = new ConnectionFactory();
        try {
            Connection connection = factory.newConnection();
            channel = connection.createChannel();
            consumeLine();
//            channel.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void consumeLine() {
        try {
            channel.queueDeclare(QueueNames.LINES, false, false, false, null);

            DeliverCallback deliverCallback = (consumerTag, delivery) -> {
                String message = new String(delivery.getBody(), "UTF-8");
                System.out.println(" [x] Received '" + message + "'");
                int wordCount = message.split(" ").length;
                System.out.println(" [x] Done");
                channel.basicPublish("", QueueNames.COUNT, null, String.valueOf(wordCount).getBytes());
            };

            channel.basicConsume(QueueNames.LINES, true, deliverCallback, consumerTag -> {});
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
