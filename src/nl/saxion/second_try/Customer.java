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
    public static void main(String[] args) throws IOException, TimeoutException {new Customer().run();}
    private static final List<String> MENU = List.of("View available rooms", "Make a reservation", "Cancel reservation", "Quit");
    private static final Scanner scanner = new java.util.Scanner(System.in);

    private Channel channel;
    private String customerQueue;
    private BlockingQueue<String> responseQueue = new ArrayBlockingQueue<>(1);

    public void run() throws IOException, TimeoutException {
        setUpRabbitMQ();
        startApplication();
    }

    public void startApplication() throws IOException {
        // display menu and get user input
        while (true) {
            System.out.println("Please select an option:");
            for (int i = 0; i < MENU.size(); i++) {
                System.out.println(i + 1 + ". " + MENU.get(i));
            }
            System.out.println("Enter your choice:");
            int choice = scanner.nextInt();

            switch (choice) {
                case 1 -> {
                    requestAvailableRooms();
                }
                case 2 -> {
                     makeReservation();
                }
                case 3 -> {
                    // cancelReservation();
                }
                case 4 -> {
                    System.out.println("Goodbye!");
                    return;
                }
                default -> {
                    System.out.println("Invalid choice, please try again.");
                }
            }
        }
    }

    /**
     * Set up RabbitMQ Logic
     */
    private void setUpRabbitMQ() throws IOException, TimeoutException {
        // 1. Connect
        // 2. Setup exchanges and queues
        // 3. Consume messages
        initializeConnection();
        consumeMessages();
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
    private void consumeMessages() throws IOException {
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

                if(!responseQueue.offer(message)){
                    System.out.println("Failed to add message to queue");
                }
            }, consumerTag -> {});
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    /**
     * Send a request to the rental agent to get available rooms
     */
    private void requestAvailableRooms() throws IOException {
        try {
            String correlationId = UUID.randomUUID().toString();
            AMQP.BasicProperties props = new AMQP.BasicProperties.Builder()
                    .correlationId(correlationId)
                    .replyTo(customerQueue)
                    .build();

            System.out.println("[x] Sending request for available rooms");
            channel.basicPublish("customer_exchange", "rental_agent_queue", true, props, "customer/request_rooms".getBytes(StandardCharsets.UTF_8));

            String response = responseQueue.poll(10, TimeUnit.SECONDS);
            if (response == null) {
                System.out.println("No response received");
            } else {
                System.out.println("Received response: " + response);
            }
        } catch (Exception e){
            e.printStackTrace();
        }
    }

    private void makeReservation() {
        // 1. Get user input for the room and building to reserve
        // 2. Send a reservation request to the rental agent
        // 3. Wait for the response (if not received, display an error message)
        // 4. Display the response (handled by consumeMessages)
        // 5. If reservation number returned, prompt confirmation (y/n)
        // 6. Send confirmation to the rental agent
        // 7. Wait for the confirmation response
        // 8. Display the confirmation response

        System.out.println("Enter the building name:");
        String building = scanner.next();
        System.out.println("Enter the room number:");
        String room = scanner.next();

        try {
            String correlationId = UUID.randomUUID().toString();
            AMQP.BasicProperties props = new AMQP.BasicProperties.Builder()
                    .correlationId(correlationId)
                    .replyTo(customerQueue)
                    .build();

            String message = "customer/make_reservation/" + building + "," + room;
            System.out.println("[x] Sending reservation request for " + building + "," + room);
            channel.basicPublish("customer_exchange", "rental_agent_queue", true, props, message.getBytes(StandardCharsets.UTF_8));

            String response = responseQueue.poll(10, TimeUnit.SECONDS);
            if (response == null) {
                System.out.println("No response received");
            } else {
                System.out.println("Received response: " + response);
                // We get a message like: Confirmed 05ebae67
                if (response.contains("Confirm")) {
                    System.out.println("Do you want to confirm the reservation? (y/n):");
                    String userResponse = scanner.next();
                    if ("y".equalsIgnoreCase(userResponse)) {
                        //
                        confirmReservation(response.split(" ")[1]);
                    } else if ("n".equalsIgnoreCase(userResponse)) {
//                        cancelReservation(response);
                    } else {
                        System.out.println("Invalid input. Reservation not confirmed.");
                    }
                }
            }
        } catch (Exception e){
            e.printStackTrace();
        }
    }

    private void confirmReservation(String reservationNumber){
        // Send a confirmation to the rental agent
        // Wait for the confirmation response
        // Display the confirmation response
        try {
            String correlationId = UUID.randomUUID().toString();
            AMQP.BasicProperties props = new AMQP.BasicProperties.Builder()
                    .correlationId(correlationId)
                    .replyTo(customerQueue)
                    .build();


            String message = "customer/confirm_reservation/" + reservationNumber;
            System.out.println("[x] Sending confirmation for reservation: " + message);
            channel.basicPublish("customer_exchange", "rental_agent_queue", true, props, message.getBytes(StandardCharsets.UTF_8));

            String response = responseQueue.poll(10, TimeUnit.SECONDS);
            if (response == null) {
                System.out.println("No response received");
            } else {
                System.out.println("Received response: " + response);
            }
        } catch (Exception e){
            e.printStackTrace();
        }
    }
}
