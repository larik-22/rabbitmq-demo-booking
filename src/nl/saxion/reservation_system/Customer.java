package nl.saxion.reservation_system;

import com.rabbitmq.client.*;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Scanner;
import java.util.UUID;

public class Customer {
    public static void main(String[] args) throws IOException {
        new Customer().run();
    }

    private static final String CUSTOMER_REQUEST_EXCHANGE = "customer_to_agent_exchange";

    private static final List<String> MENU = List.of("View available rooms", "Make a reservation", "Cancel reservation", "Quit");
    private static final Scanner scanner = new Scanner(System.in);
    private Channel channel;

    public void run() throws IOException {
        initializeConnection();

        outer:
        while (true) {
            System.out.println("Please select an option:");
            for (int i = 0; i < MENU.size(); i++) {
                System.out.println(i + 1 + ". " + MENU.get(i));
            }
            System.out.println("Enter your choice:");
            int choice = scanner.nextInt();
            switch (choice) {
                case 1:
                    viewAvailableRooms();
                    break;
                case 2:
//                    makeReservation();
                    break;
                case 3:
//                    cancelReservation();
                    break;
                case 4:
                    System.out.println("Goodbye!");
                    return;
                default:
                    System.out.println("Invalid choice, please try again.");
            }
        }
    }

    private void initializeConnection() {
        try {
            ConnectionFactory factory = new ConnectionFactory();
            channel = factory.newConnection().createChannel();
        } catch (Exception e) {
            throw new RuntimeException("Failed to connect to RabbitMQ", e);
        }
    }

    private void viewAvailableRooms() throws IOException {
        String replyQueueName = channel.queueDeclare().getQueue();
        String correlationId = UUID.randomUUID().toString();

        // Send request to rental agent queue via direct exchange
        AMQP.BasicProperties props = new AMQP.BasicProperties
                .Builder()
                .correlationId(correlationId)
                .replyTo(replyQueueName)
                .build();

        String message = "Requesting list of buildings";
        channel.basicPublish(CUSTOMER_REQUEST_EXCHANGE, "rental_agent_request", props, message.getBytes());

        // Listen for the reply
        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            if (delivery.getProperties().getCorrelationId().equals(correlationId)) {
                String response = new String(delivery.getBody(), StandardCharsets.UTF_8);
                System.out.println("Available rooms: \n" + response);
            }
        };

        channel.basicConsume(replyQueueName, true, deliverCallback, consumerTag -> {
        });
    }
}
