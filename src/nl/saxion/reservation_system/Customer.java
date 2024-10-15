package nl.saxion.reservation_system;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.*;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

public class Customer {
    public static void main(String[] args) throws IOException, InterruptedException {
        new Customer().run();
    }

    private static final BlockingQueue<String> responseQueue = new ArrayBlockingQueue<>(1);
    private static final String CUSTOMER_REQUEST_EXCHANGE = "customer_to_agent_exchange";
    private static final String RESERVATION_EXCHANGE = "reservation_exchange";
    private static final List<String> MENU = List.of("View available rooms", "Make a reservation", "Cancel reservation", "Quit");
    private static final Scanner scanner = new Scanner(System.in);

    private Channel channel;
    private String replyQueueName;

    public void run() throws IOException, InterruptedException {
        initializeConnection();
        // listen for responses from rental agent

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
                    makeReservation();
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
            setupResponseConsumer();
        } catch (Exception e) {
            throw new RuntimeException("Failed to connect to RabbitMQ", e);
        }
    }

    private void setupResponseConsumer() throws IOException {
        replyQueueName = channel.queueDeclare().getQueue();
        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            String correlationId = delivery.getProperties().getCorrelationId();
            String response = new String(delivery.getBody(), StandardCharsets.UTF_8);
            System.out.println("Received response: " + response);
            responseQueue.add(response);
        };

        channel.basicConsume(replyQueueName, true, deliverCallback, consumerTag -> {});
    }

    private void viewAvailableRooms() throws IOException {
        String correlationId = UUID.randomUUID().toString();

        // Send request to rental agent queue via direct exchange
        AMQP.BasicProperties props = new AMQP.BasicProperties
                .Builder()
                .correlationId(correlationId)
                .replyTo(replyQueueName)
                .build();

        String message = "Requesting list of buildings";
        channel.basicPublish(CUSTOMER_REQUEST_EXCHANGE, "rental_agent_request", props, message.getBytes());

        // Wait for the reply
        try {
            String response = responseQueue.take();
            System.out.println("Available rooms: \n" + response);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Failed to receive response", e);
        }
    }

    private final BlockingQueue<String> confirmationQueue = new ArrayBlockingQueue<>(1);

    private void makeReservation() throws IOException, InterruptedException {
        System.out.println("Enter the building name you want to reserve a room in:");
        String buildingName = scanner.next();
        System.out.println("Enter the room name you want to reserve:");
        String roomName = scanner.next();

        // Send reservation request to Rental Agent
        sendReservationRequest(buildingName, roomName);

        // Wait for the reservation number or unavailability message
        String response = confirmationQueue.take();  // Blocking call, waits for confirmation

        if (response.startsWith("Reservation number")) {
            System.out.println(response);
            handleConfirmation(response);
        } else {
            System.out.println(response);  // "Room unavailable"
        }
    }

    private void sendReservationRequest(String buildingName, String roomName) throws IOException {
        String correlationId = UUID.randomUUID().toString();

        Map<String, String> request = new HashMap<>();
        request.put("buildingName", buildingName);
        request.put("roomName", roomName);
        request.put("replyTo", replyQueueName);
        request.put("correlationId", correlationId);

        ObjectMapper objectMapper = new ObjectMapper();
        String requestJson = objectMapper.writeValueAsString(request);

        // Publish the reservation request to the rental agent's exchange
        System.out.println("Sending reservation request: " + requestJson);
        AMQP.BasicProperties props = new AMQP.BasicProperties.Builder()
                .correlationId(correlationId)
                .replyTo(replyQueueName)
                .build();

        channel.basicPublish(RESERVATION_EXCHANGE, "", props, requestJson.getBytes(StandardCharsets.UTF_8));
    }

    // Handles reservation confirmation
    private void handleConfirmation(String reservationNumber) throws InterruptedException {
        System.out.println("Do you want to confirm your reservation? (Y/N): ");
        String confirmation = scanner.next();

        // Send confirmation message back to rental agent
        if ("Y".equalsIgnoreCase(confirmation)) {
            confirmationQueue.put("CONFIRMED: " + reservationNumber);
        } else if ("N".equalsIgnoreCase(confirmation)) {
            confirmationQueue.put("CANCELLED: " + reservationNumber);
        } else {
            System.out.println("Invalid input, please try again.");
            handleConfirmation(reservationNumber);
        }
    }
}