package nl.saxion.second_try;

import com.rabbitmq.client.*;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Scanner;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class Customer {
    public static void main(String[] args) throws IOException, TimeoutException {
        new Customer().run();
    }

    private static final List<String> MENU = List.of("View available rooms", "Make a reservation", "Cancel reservation", "Quit");
    private static final Scanner scanner = new java.util.Scanner(System.in);

    private Channel channel;
    private String customerQueue;
    private final BlockingQueue<String> responseQueue = new ArrayBlockingQueue<>(1);


    public void run() throws IOException, TimeoutException {
        setUpRabbitMQ();
        startApplication();
    }

    /**
     * Set up RabbitMQ Logic
     */
    private void setUpRabbitMQ() throws IOException, TimeoutException {
        initializeConnection();
        startListeningForMessages();
    }

    /**
     * Start the application
     */
    public void startApplication() throws IOException {
        // display menu and get user input
        while (true) {
            System.out.println("Please select an option:");
            for (int i = 0; i < MENU.size(); i++) {
                System.out.println(i + 1 + ". " + MENU.get(i));
            }

            System.out.println("Enter your choice:");

            try {
                int choice = scanner.nextInt();
                switch (choice) {
                    case 1 -> requestAvailableRooms();
                    case 2 -> makeReservation();
                    case 3 -> cancelReservation(null);
                    case 4 -> {
                        System.out.println("Goodbye!");
                        return;
                    }
                    default -> System.out.println("Invalid choice, please try again.");
                }
            } catch (Exception e) {
                System.out.println("Invalid choice, please try again.");
                scanner.next();
            }
        }
    }

    /**
     * Connect to RabbitMQ
     */
    private void initializeConnection() throws IOException, TimeoutException {
        ConnectionFactory factory = new ConnectionFactory();
        Connection connection = factory.newConnection();
        this.channel = connection.createChannel();
    }

    /**
     * Consume messages from the queue (Listen for any response)
     */
    private void startListeningForMessages() throws IOException {
        // 1. Create a queue
        // 2. Bind the queue to the exchanges
        // 3. Consume messages
        // Declare exchanges
        channel.exchangeDeclare("customer_exchange", BuiltinExchangeType.DIRECT);

        // Create a unique queue for each customer to receive messages
        customerQueue = channel.queueDeclare("Customer " + UUID.randomUUID().toString().substring(0, 8), false, false, false, null).getQueue();
        channel.queueBind(customerQueue, "customer_exchange", "");

        try {
            // receive messages from the queue
            channel.basicConsume(customerQueue, true, (consumerTag, delivery) -> {
                String message = new String(delivery.getBody(), StandardCharsets.UTF_8);

                if (!responseQueue.offer(message)) {
                    System.out.println("Failed to add message to queue");
                }
            }, consumerTag -> {
            });
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    /**
     * Send a message to the rental agent and wait for the response.
     * If no response is received, display an error message
     *
     * @param props   The properties to send with the message (replyTo queue and correlationId)
     * @param message The message to send
     */
    private void sendMessageAndAwaitResponse(AMQP.BasicProperties props, String message) throws IOException, InterruptedException {
        channel.basicPublish("customer_exchange", "rental_agent_queue", true, props, message.getBytes(StandardCharsets.UTF_8));

        String response = responseQueue.poll(5, TimeUnit.SECONDS);
        if (response == null) {
            System.out.println("No response received");
        } else {
            System.out.println("Received response: " + response);
        }
    }

    /**
     * Generates AMPQ properties for the message
     *
     * @param replyTo The queue to send the response to
     * @return The properties to send with the message
     */
    private AMQP.BasicProperties getProps(String replyTo) {
        String correlationId = UUID.randomUUID().toString();
        return new AMQP.BasicProperties.Builder()
                .correlationId(correlationId)
                .replyTo(replyTo)
                .build();
    }

    /**
     * Send a request to the rental agent to get available rooms
     */
    private void requestAvailableRooms() {
        try {
            System.out.println("[x] Sending request for available rooms");

            AMQP.BasicProperties props = getProps(customerQueue);
            sendMessageAndAwaitResponse(props, "customer/request_rooms");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * Makes a reservation. Prompts the user for the building and room number
     * and sends a reservation request to the rental agent. Waits for the response and based on it
     * proceeds with confirmation or displays an error message.
     */
    private void makeReservation() {
        System.out.println("Enter the building name:");
        String building = scanner.next();
        System.out.println("Enter the room number:");
        String room = scanner.next();

        try {
            System.out.println("[x] Sending reservation request for " + building + "," + room);

            AMQP.BasicProperties props = getProps(customerQueue);
            String message = "customer/make_reservation/" + building + "," + room;
            channel.basicPublish("customer_exchange", "rental_agent_queue", true, props, message.getBytes(StandardCharsets.UTF_8));

            String response = responseQueue.poll(5, TimeUnit.SECONDS);
            if (response == null) {
                System.out.println("No response received");
            } else {
                System.out.println("Received response: " + response);

                // We get a message like: ReservationNr 05ebae67
                if (response.contains("ReservationNr")) {
                    System.out.println("Do you want to confirm the reservation? (y/n):");
                    String userResponse = scanner.next();

                    if ("y".equalsIgnoreCase(userResponse)) {
                        confirmReservation(response.split(" ")[1]);
                    } else {
                        cancelReservation(response.split(" ")[1]);
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * Send a confirmation request to the rental agent
     * Wait for the response and display it
     *
     * @param reservationNumber The reservation number to confirm
     */
    private void confirmReservation(String reservationNumber) {
        try {
            String message = "customer/confirm_reservation/" + reservationNumber;
            System.out.println("[x] Sending confirmation for reservation: " + message);

            AMQP.BasicProperties props = getProps(customerQueue);
            sendMessageAndAwaitResponse(props, message);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * Cancel a reservation
     * If the reservation number is not provided, prompt the user for it
     *
     * @param reservationNumber The reservation number to cancel
     */
    private void cancelReservation(String reservationNumber) {
        if (reservationNumber == null) {
            System.out.println("Enter the reservation number to cancel:");
            reservationNumber = scanner.next();
        }

        try {
            AMQP.BasicProperties props = getProps(customerQueue);
            String message = "customer/cancel_reservation/" + reservationNumber;
            System.out.println("[x] Sending cancel reservation request for " + reservationNumber);

            sendMessageAndAwaitResponse(props, message);
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
