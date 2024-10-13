package nl.saxion.exercise_2;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeoutException;

public class FileWordCounterAgent {
    private String fileName;
    private Channel channel;
    private int totalCount = 0;

    public FileWordCounterAgent(String fileName) {
        this.fileName = fileName;
    }

    public static void main(String[] args) {
        new FileWordCounterAgent(args.length > 0 ? args[0] : "resources/alice_in_wonderland.txt").countWordsInFile();
    }

    /**
     * this method opens the file filename
     * for each line in the file it wil send a message to rabbitMQ
     * for each line sent, it expects that it will receive a return message
     * containing the number of words in the line.
     * When the whole file has been processed it will output the word count,
     * close the connection with RabbitMQ and exit the application.
     */
    private void countWordsInFile() {
        ConnectionFactory factory = new ConnectionFactory();
        try {
            Connection connection = factory.newConnection();
            channel = connection.createChannel();
            startCounting();
            getCount();

            System.out.println("Total word count: " + totalCount);
//            channel.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void startCounting() throws IOException, TimeoutException {
        System.out.println("Counting words in file " + fileName);

        File file = new File(fileName);
        if (!file.exists()) {
            System.out.println("File " + fileName + " does not exist");
            return;
        }

        channel.queueDeclare(QueueNames.LINES, false, false, false, null);
        channel.queuePurge(QueueNames.LINES);

        BufferedReader reader;

        try {
            reader = new BufferedReader(new FileReader(fileName));
            String line = reader.readLine();

            while (line != null) {
                System.out.println("Sending: " + line);
                channel.basicPublish("", QueueNames.LINES, null, line.getBytes(StandardCharsets.UTF_8));
                line = reader.readLine();
            }

            reader.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void getCount() throws IOException {
        channel.queueDeclare(QueueNames.COUNT, false, false, false, null);

        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            String message = new String(delivery.getBody(), "UTF-8");
            totalCount += Integer.parseInt(message);
            System.out.println(" [x] Received '" + message + "'" + " Total: " + totalCount);
        };

        channel.basicConsume(QueueNames.COUNT, true, deliverCallback, consumerTag -> {});
    }
}
