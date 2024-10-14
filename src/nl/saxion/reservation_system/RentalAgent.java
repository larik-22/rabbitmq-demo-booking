package nl.saxion.reservation_system;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.*;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeoutException;

public class RentalAgent {
    public static void main(String[] args) throws IOException, TimeoutException {
        new RentalAgent("Agent " + java.util.UUID.randomUUID().toString().substring(0, 8)).run();
    }

    private Channel channel;
    private static final String BUILDING_HEARTBEAT_EXCHANGE = "building_heartbeat_exchange";
    private static final String AGENT_REQUEST_QUEUE = "rental_agent_queue";
    private static final String CUSTOMER_REQUEST_EXCHANGE = "customer_to_agent_exchange";
    private static final String CUSTOMER_REPLY_QUEUE = "agent_to_customer_queue";
    private static final String CUSTOMER_REPLY_EXCHANGE = "agent_to_customer_exchange";

    private final String name;
    private final HashMap<String, String> pendingReservations; // Track reservations (Reservation number -> Building)
    private final List<String> activeReservations; // Store confirmed reservations
    private final Map<String, Map<String, Boolean>> knownBuildings; // Building name -> Rooms -> Availability
    private final Map<String, Long> lastHeartbeatTimestamps = new ConcurrentHashMap<>();

    public RentalAgent(String name) {
        this.name = name;
        this.pendingReservations = new HashMap<>();
        this.activeReservations = new ArrayList<>();
        this.knownBuildings = new HashMap<>();
    }

    public void run() throws IOException, TimeoutException {
        setupRabbitMq();
    }

    private void setupRabbitMq() throws IOException, TimeoutException {
        initializeConnection();
        setupHeartbeatListener();
        setupCustomerRequestListener();
    }

    private void initializeConnection() throws IOException, TimeoutException {
        ConnectionFactory factory = new ConnectionFactory();
        Connection connection = factory.newConnection();
        this.channel = connection.createChannel();
    }

    private void setupHeartbeatListener() throws IOException {
        channel.exchangeDeclare(BUILDING_HEARTBEAT_EXCHANGE, BuiltinExchangeType.FANOUT);

        String heartbeatQueue = "agent_heartbeat_queue_" + name;
        channel.queueDeclare(heartbeatQueue, false, false, false, null);
        channel.queueBind(heartbeatQueue, BUILDING_HEARTBEAT_EXCHANGE, "");

        DeliverCallback heartbeatCallback = (consumerTag, delivery) -> {
            String message = new String(delivery.getBody(), StandardCharsets.UTF_8);
//            System.out.println(" [x] Received heartbeat: '" + message + "'");
            updateBuildingList(message);
        };

        channel.basicConsume(heartbeatQueue, true, heartbeatCallback, consumerTag -> {
        });
    }

    private void setupCustomerRequestListener() throws IOException {
        channel.exchangeDeclare(CUSTOMER_REQUEST_EXCHANGE, BuiltinExchangeType.DIRECT);
        channel.queueDeclare(AGENT_REQUEST_QUEUE, false, false, false, null);
        channel.queueBind(AGENT_REQUEST_QUEUE, CUSTOMER_REQUEST_EXCHANGE, "rental_agent_request");

        // 1 for fair dispatching
        channel.basicQos(1);

        DeliverCallback customerRequestCallback = (consumerTag, delivery) -> {
            String message = new String(delivery.getBody(), StandardCharsets.UTF_8);
            System.out.println(" [x] Customer Request: '" + message + "'");
            respondToCustomerRequest(channel, delivery.getProperties().getReplyTo(), delivery.getProperties().getCorrelationId());

            // Acknowledge the message after processing
            channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
        };

        // Disable auto-acknowledge
        channel.basicConsume(AGENT_REQUEST_QUEUE, false, customerRequestCallback, consumerTag -> {});
    }

    private void updateBuildingList(String message) {
        // Assuming format: Sending heartbeat: {"rooms":{"Room 2":true,"Room 1":true,"Room 0":true},"building":"Building a15fb35c","timestamp":1728904189706}
        try {
            ObjectMapper objectMapper = new ObjectMapper();
            Map<String, Object> parsedMessage = objectMapper.readValue(message, Map.class);

            String buildingName = (String) parsedMessage.get("building");
            Map<String, Boolean> rooms = (Map<String, Boolean>) parsedMessage.get("rooms");
            long timestamp = (long) parsedMessage.get("timestamp");

            // Update the buildings map and last heartbeat timestamp
            knownBuildings.put(buildingName, rooms);
            lastHeartbeatTimestamps.put(buildingName, timestamp);

            // Remove buildings that haven't sent a heartbeat in the last 5 seconds
            long currentTime = System.currentTimeMillis();
            lastHeartbeatTimestamps.entrySet().removeIf(entry -> (currentTime - entry.getValue()) > 5000);
            knownBuildings.keySet().removeIf(building -> !lastHeartbeatTimestamps.containsKey(building));

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    // Respond to customer requests by sending the list of buildings
    private void respondToCustomerRequest(Channel channel, String replyQueue, String correlationId) throws IOException {
        long currentTime = System.currentTimeMillis();

        // Step 1: Filter the buildings based on the timestamp
        // Those buildings that haven't sent a response within the last 5 seconds are considered offline
        StringBuilder responseBuilder = new StringBuilder();
        for (Map.Entry<String, Map<String, Boolean>> buildingEntry : knownBuildings.entrySet()) {
            long lastTimestamp = lastHeartbeatTimestamps.getOrDefault(buildingEntry.getKey(), 0L);
            if ((currentTime - lastTimestamp) <= 5000) {
                responseBuilder
                        .append("Building: ").append(buildingEntry.getKey())
                        .append(", Rooms: ").append(buildingEntry.getValue()).append("\n");
            }
        }

        String response = responseBuilder.toString();

        // Step 2: Send the response back to the customer via the replyTo queue
        AMQP.BasicProperties replyProps = new AMQP.BasicProperties.Builder()
                .correlationId(correlationId)
                .build();

        channel.basicPublish("", replyQueue, replyProps, response.getBytes(StandardCharsets.UTF_8));
        System.out.println(" [x] Sent building list to customer");
    }
}
