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
    private static final String BUILDING_HEARTBEAT_ROUTING_KEY = "building_heartbeat";
    private static final String AGENT_REQUEST_QUEUE = "rental_agent_queue";
    private static final String CUSTOMER_REQUEST_EXCHANGE = "customer_to_agent_exchange";
    private static final String CUSTOMER_REPLY_QUEUE = "agent_to_customer_queue";
    private static final String CUSTOMER_REPLY_EXCHANGE = "agent_to_customer_exchange";

    private final String name;
    //    private final List<String> knownBuildings; // Keeps track of buildings the agent can interact with
    private final HashMap<String, String> pendingReservations; // Track reservations (Reservation number -> Building)
    private final List<String> activeReservations;
    private final Map<String, Map<String, Boolean>> buildings;// Store confirmed reservations

    public RentalAgent(String name) {
        this.name = name;
//        this.knownBuildings = new ArrayList<>();
        this.pendingReservations = new HashMap<>();
        this.activeReservations = new ArrayList<>();
        this.buildings = new HashMap<>();
    }

    public void run() throws IOException, TimeoutException {
        setupRabbitMq();
    }
    public void setupRabbitMq() throws IOException, TimeoutException {
        ConnectionFactory factory = new ConnectionFactory();
        Connection connection = factory.newConnection();
        this.channel = connection.createChannel();

        // Declare the exchange for receiving heartbeats from buildings
        channel.exchangeDeclare(BUILDING_HEARTBEAT_EXCHANGE, BuiltinExchangeType.DIRECT);

        // Queue for listening to heartbeats
        String heartbeatQueue = "agent_heartbeat_queue_" + name;
        channel.queueDeclare(heartbeatQueue, false, false, false, null);
        channel.queueBind(heartbeatQueue, BUILDING_HEARTBEAT_EXCHANGE, BUILDING_HEARTBEAT_ROUTING_KEY);

        // Listen for heartbeats from buildings
        DeliverCallback heartbeatCallback = (consumerTag, delivery) -> {
            String message = new String(delivery.getBody(), StandardCharsets.UTF_8);
            System.out.println(" [x] Received Heartbeat: '" + message + "'");

            // Update building information
            updateBuildingList(message);
        };

        channel.basicConsume(heartbeatQueue, true, heartbeatCallback, consumerTag -> {
        });

//    // Declare a queue for customer requests
//    channel.queueDeclare(AGENT_REQUEST_QUEUE, false, false, false, null);
//
//    // Listen for customer requests
//    DeliverCallback customerRequestCallback = (consumerTag, delivery) -> {
//        String message = new String(delivery.getBody(), StandardCharsets.UTF_8);
//        System.out.println(" [x] Customer Request: '" + message + "'");
//
//        // Send the list of buildings back to the customer
//        respondToCustomerRequest(channel, delivery.getProperties().getReplyTo(), delivery.getProperties().getCorrelationId());
//    };
//    channel.basicConsume(AGENT_REQUEST_QUEUE, true, customerRequestCallback, consumerTag -> {});
    }


    // src/nl/saxion/reservation_system/RentalAgent.java


    private final Map<String, Long> lastHeartbeatTimestamps = new ConcurrentHashMap<>();

    private void updateBuildingList(String message) {
        try {
            ObjectMapper objectMapper = new ObjectMapper();
            Map<String, Object> parsedMessage = objectMapper.readValue(message, Map.class);

            String buildingName = (String) parsedMessage.get("building");
            Map<String, Boolean> rooms = (Map<String, Boolean>) parsedMessage.get("rooms");
            long timestamp = (long) parsedMessage.get("timestamp");

            // Update the buildings map and last heartbeat timestamp
            buildings.put(buildingName, rooms);
            lastHeartbeatTimestamps.put(buildingName, timestamp);
            System.out.println("Updated Building List: " + buildings);
            System.out.println("Last Heartbeat Timestamps: " + lastHeartbeatTimestamps);

            long currentTime = System.currentTimeMillis();
            lastHeartbeatTimestamps.entrySet().removeIf(entry -> (currentTime - entry.getValue()) > 5000);
            buildings.keySet().removeIf(building -> !lastHeartbeatTimestamps.containsKey(building));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    // Respond to customer requests by sending the list of buildings
    private void respondToCustomerRequest(Channel channel, String replyQueue, String correlationId) throws IOException {
        // Convert the internal list of buildings to a string format
        StringBuilder responseBuilder = new StringBuilder();
        for (Map.Entry<String, Map<String, Boolean>> buildingEntry : buildings.entrySet()) {
            responseBuilder.append("Building: ").append(buildingEntry.getKey()).append(", Rooms: ").append(buildingEntry.getValue()).append("\n");
        }

        String response = responseBuilder.toString();

        // Send the response back to the customer using the reply-to pattern
        AMQP.BasicProperties replyProps = new AMQP.BasicProperties.Builder()
                .correlationId(correlationId)
                .build();

        channel.basicPublish("", replyQueue, replyProps, response.getBytes(StandardCharsets.UTF_8));
        System.out.println(" [x] Sent building list to customer");
    }
}
