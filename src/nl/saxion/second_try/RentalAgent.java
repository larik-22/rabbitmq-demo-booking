package nl.saxion.second_try;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.*;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.*;

public class RentalAgent {
    public static void main(String[] args) throws IOException, TimeoutException {
        new RentalAgent("Agent " + UUID.randomUUID().toString().substring(0, 8)).run();
    }

    public RentalAgent(String name) {
        this.name = name;
    }

    private final String name;
    private final Map<String, Map<String, Boolean>> knownBuildings = new ConcurrentHashMap<>(); // Building name -> Rooms -> Availability
    private final Map<String, Long> lastHeartbeatTimestamps = new ConcurrentHashMap<>();
    private final Map<String, Map<String, Reservation>> reservations = new ConcurrentHashMap<>(); // building -> room/reservation
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);

    private String uniqueQueueName;
    private Channel channel;
    private final long BUILDING_TIMEOUT = 5000;
    private final DeliverCallback deliverCallback = (consumerTag, delivery) -> {
        String message = new String(delivery.getBody(), StandardCharsets.UTF_8);
        processMessage(message.split("/"), delivery);
    };

    public void run() throws IOException, TimeoutException {
        setUpRabbitMQ();
    }

    // Each agent has a unique name, which is used to create a queue. This queue is bind to different exchanges.
    // Message are sent to the exchange, and the agent listens to the queue.
    private void setUpRabbitMQ() throws IOException, TimeoutException {
        // 1. Connect
        initializeConnection();
        // 2. Consume messages and process them
        startListeningForMessages();
    }

    private void initializeConnection() throws IOException, TimeoutException {
        ConnectionFactory factory = new ConnectionFactory();
        Connection connection = factory.newConnection();
        this.channel = connection.createChannel();
    }

    private void startListeningForMessages() throws IOException {
        channel.basicQos(1);

        // uniqueQueueName is used to create a unique queue for each agent
        uniqueQueueName = channel.queueDeclare().getQueue();

        // Exchanges
        channel.exchangeDeclare("heartbeat_exchange", BuiltinExchangeType.FANOUT);
        channel.exchangeDeclare("building_exchange", BuiltinExchangeType.DIRECT);

        // Bind queues;
        channel.queueDeclare("rental_agent_queue", false, false, false, null);

        channel.queueBind(uniqueQueueName, "heartbeat_exchange", "");
        channel.queueBind(uniqueQueueName, "building_exchange", "rental_agent_queue");

        // Edge case with new rental agent who got the request before the building was created
        // Don't bind to customer exchange if no buildings are known
        scheduler.schedule(this::bindAgentToCustomerQueue, 1, TimeUnit.SECONDS);

        channel.basicConsume(uniqueQueueName, true, deliverCallback, consumerTag -> {
        });
    }

    /**
     * Bind the agent to the customer queue, only if there are known buildings
     * Otherwise, retry after 1 second
     */
    private void bindAgentToCustomerQueue() {
        if (!knownBuildings.isEmpty()) {
            try {
                channel.exchangeDeclare("customer_exchange", BuiltinExchangeType.DIRECT);
                channel.queueBind("rental_agent_queue", "customer_exchange", "rental_agent_queue");
                channel.basicConsume("rental_agent_queue", true, deliverCallback, consumerTag -> {
                });
                scheduler.shutdown();
            } catch (IOException e) {
                e.printStackTrace();
            }
        } else {
            scheduler.schedule(this::bindAgentToCustomerQueue, 1, TimeUnit.SECONDS);
        }
    }

    /**
     * Create AMPQ properties for the message, so building can respond to the agent
     *
     * @param correlationId the correlationId of the customer
     * @return the properties for the message
     */
    private AMQP.BasicProperties createProperties(String correlationId) {
        return new AMQP.BasicProperties.Builder()
                .correlationId(correlationId)
                .replyTo(uniqueQueueName)
                .build();
    }

    /**
     * Get AMPQ properties from the delivery and create new properties for the reply message
     *
     * @param delivery the delivery object of the message containing replyTo and correlationId of the customer
     * @return the properties for the reply message
     */
    private AMQP.BasicProperties getReplyProps(Delivery delivery) {
        String correlationId = delivery.getProperties().getCorrelationId();

        return new AMQP.BasicProperties.Builder()
                .correlationId(correlationId)
                .build();
    }

    /**
     * Get AMPQ id and create new properties for the reply message
     *
     * @param correlationId the correlationId of the customer
     * @return the properties for the reply message
     */
    private AMQP.BasicProperties getReplyProps(String correlationId) {
        return new AMQP.BasicProperties.Builder()
                .correlationId(correlationId)
                .build();
    }

    /**
     * Process the consumed message
     * Follows the pattern of the message: [sender]/[type]/[content]
     *
     * @param messageParts the parts of the message, split by "/"
     * @param delivery     the delivery object of the message containing replyTo and correlationId of the customer
     */
    private void processMessage(String[] messageParts, Delivery delivery) {
        String sender = messageParts[0].toLowerCase();
        String messageType = messageParts[1].toLowerCase();
        String content = messageParts.length > 2 ? messageParts[2] : "";

        System.out.println("[x] Received message from " + sender + ": " + content);

        switch (sender) {
            case "building" -> {
                //Process building message
                switch (messageType) {
                    case "heartbeat" -> updateBuildings(content);
                    case "reservation_response" -> processReservationResponse(content);
                    case "reservation_finalized" -> finalizeReservation(content);
                    case "reservation_cancelled" -> finalizeCancellation(content);
                }
            }
            case "customer" -> {
                //Process customer message
                switch (messageType) {
                    case "request_rooms" -> sendAvailableRooms(delivery);
                    case "make_reservation" -> queryBuildingAvailability(content, delivery);
                    case "confirm_reservation" -> processConfirmation(content, delivery);
                    case "cancel_reservation" -> processCancellation(content, delivery);
                }
            }

        }
    }

    /**
     * Update the known buildings and reservations
     * If a building hasn't sent a heartbeat in the last 5 seconds, remove it from the list
     * As well as the reservations for that building
     *
     * @param message the message containing the building information
     */
    private void updateBuildings(String message) {
        try {
            ObjectMapper objectMapper = new ObjectMapper();
            Map<String, Object> parsedMessage = objectMapper.readValue(message, Map.class);

            // Extract building name, rooms, timestamp, and reservations
            String buildingName = (String) parsedMessage.get("building");
            Map<String, Boolean> rooms = (Map<String, Boolean>) parsedMessage.get("rooms");
            String reservationsJson = objectMapper.writeValueAsString(parsedMessage.get("reservations"));
            long timestamp = (long) parsedMessage.get("timestamp");

            // Update the buildings map and last heartbeat timestamp
            knownBuildings.put(buildingName, rooms);
            reservations.put(buildingName, Reservation.fromJson(reservationsJson));
            lastHeartbeatTimestamps.put(buildingName, timestamp);

            // Remove buildings and reservations that haven't sent a heartbeat in the last 5 seconds
            long currentTime = System.currentTimeMillis();
            lastHeartbeatTimestamps.entrySet().removeIf(entry -> (currentTime - entry.getValue()) > BUILDING_TIMEOUT);
            knownBuildings.keySet().removeIf(building -> !lastHeartbeatTimestamps.containsKey(building));
            reservations.keySet().removeIf(building -> !lastHeartbeatTimestamps.containsKey(building));

            //TODO: uncomment to debug
//            System.out.println("Updated building list: " + knownBuildings);
//            System.out.println("Updated reservation list: " + reservations);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Send a message to the customer containing the available rooms
     *
     * @param delivery the delivery object of the message containing replyTo and correlationId of the customer
     */
    private void sendAvailableRooms(Delivery delivery) {
        try {
            String customerQueue = delivery.getProperties().getReplyTo();
            String response = getAvailableRoomsJson();
            response = response.isEmpty() ? "No rooms available" : response;

            AMQP.BasicProperties props = getReplyProps(delivery);
            channel.basicPublish("", customerQueue, props, response.getBytes(StandardCharsets.UTF_8));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Process the available rooms and return them as a JSON string.
     * If a building hasn't sent a heartbeat in the last 5 seconds, it is considered unavailable and not included in the response
     *
     * @return a JSON string containing the available rooms
     */
    private String getAvailableRoomsJson() {
        long currentTime = System.currentTimeMillis();

        StringBuilder responseBuilder = new StringBuilder();
        for (Map.Entry<String, Map<String, Boolean>> buildingEntry : knownBuildings.entrySet()) {
            long lastTimestamp = lastHeartbeatTimestamps.getOrDefault(buildingEntry.getKey(), 0L);

            if ((currentTime - lastTimestamp) <= BUILDING_TIMEOUT) {
                responseBuilder
                        .append("Building: ").append(buildingEntry.getKey())
                        .append(", Rooms: ").append(buildingEntry.getValue()).append("\n");
            }
        }

        return responseBuilder.toString();
    }

    /**
     * Query the building for room availability
     *
     * @param content  the content of the message containing the building and room number
     * @param delivery the delivery object of the message containing replyTo and correlationId of the customer
     */
    private void queryBuildingAvailability(String content, Delivery delivery) {
        String building = content.split(",")[0];
        String room = content.split(",")[1];

        if (knownBuildings.containsKey(building) && knownBuildings.get(building).containsKey(room)) {
            // check if room number corresponds to a room in the building
            try {
                // customer's correlationId and replyTo saved for later
                String correlationId = delivery.getProperties().getCorrelationId();
                String customerQueue = delivery.getProperties().getReplyTo();

                AMQP.BasicProperties props = createProperties(correlationId);
                String message = "rental_agent/reservation_request/" + building + "," + room + "," + correlationId + "," + customerQueue;

                System.out.println("[x] Forwarding reservation request to building: " + message);
                channel.basicPublish("building_exchange", building, true, props, message.getBytes(StandardCharsets.UTF_8));
            } catch (IOException e) {
                e.printStackTrace();
            }
        } else {
            // respond immediately if building or room not found
            replyImmediately(delivery.getProperties().getReplyTo(), delivery.getProperties().getCorrelationId(), "Building or room not found");
        }
    }

    /**
     * Process the reservation response from the building
     *
     * @param content the content of the message containing the response, correlationId, and customerQueue
     */
    private void processReservationResponse(String content) {
        try {
            String[] parts = content.split(",");
            String response = parts[2];
            String correlationId = parts[3];
            String customerQueue = parts[4];

            AMQP.BasicProperties props = createProperties(correlationId);

            System.out.println("[x] Forwarding response to customer: " + content);
            channel.basicPublish("", customerQueue, props, response.getBytes(StandardCharsets.UTF_8));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Process the confirmation from the building
     *
     * @param content  the content of the message containing the reservationId
     * @param delivery the delivery object of the message containing replyTo and correlationId of the customer
     */
    private void processConfirmation(String content, Delivery delivery) {
        String reservationId = content;
        String replyTo = delivery.getProperties().getReplyTo();
        String correlationId = delivery.getProperties().getCorrelationId();

        // Reservations: {Building_8a18eee0={room_0=Reservation{id='7f065ef1', finalized=false}}}
        boolean found = false;
        for (String building : reservations.keySet()) {
            if (found) {
                break;
            }

            for (String room : reservations.get(building).keySet()) {
                if (reservations.get(building).get(room).getId().equals(reservationId)) {
                    try {
                        AMQP.BasicProperties props = createProperties(correlationId);
                        String message = "building/confirm_reservation/" + reservationId + "," + correlationId + "," + replyTo;

                        System.out.println("[x] Forwarding confirmation to building: " + message);
                        channel.basicPublish("building_exchange", building, true, props, message.getBytes(StandardCharsets.UTF_8));
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                    found = true;
                    break;
                }
            }
        }

        if (!found) {
            replyImmediately(replyTo, correlationId, "Reservation not found");
        }
    }

    /**
     * Reply to the customer immediately
     *
     * @param replyTo       customer queue
     * @param correlationId customer correlationID
     * @param response      response message
     */
    private void replyImmediately(String replyTo, String correlationId, String response) {
        try {
            AMQP.BasicProperties props = getReplyProps(correlationId);
            System.out.println("[x] Responding to customer: " + response);

            channel.basicPublish("", replyTo, props, response.getBytes(StandardCharsets.UTF_8));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Finalize the reservation and respond to the customer
     *
     * @param content the content of the message sent from building
     *                containing the message, reservationId, correlationId, and customerQueue
     */
    private void finalizeReservation(String content) {
        String message = content.split(",")[0];
        String reservationId = content.split(",")[1];
        String correlationId = content.split(",")[2];
        String customerQueue = content.split(",")[3];

        try {
            AMQP.BasicProperties props = getReplyProps(correlationId);
            String response = message + " " + reservationId;

            System.out.println("[x] Responding to customer: " + response);
            channel.basicPublish("", customerQueue, props, response.getBytes(StandardCharsets.UTF_8));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Process the cancellation request from the customer
     * Forward the cancellation to the building
     *
     * @param content  the content of the message containing the reservationId
     * @param delivery the delivery object of the message containing replyTo and correlationId of the customer
     */
    private void processCancellation(String content, Delivery delivery) {
        String reservationId = content.split(",")[0];
        String replyTo = delivery.getProperties().getReplyTo();
        String correlationId = delivery.getProperties().getCorrelationId();

        boolean found = false;

        for (String building : reservations.keySet()) {
            if (found) {
                break;
            }

            for (String room : reservations.get(building).keySet()) {
                if (reservations.get(building).get(room).getId().equals(reservationId)) {
                    try {
                        AMQP.BasicProperties props = createProperties(correlationId);
                        String message = "building/cancel_reservation/" + reservationId + "," + room + "," + correlationId + "," + replyTo;

                        System.out.println("[x] Forwarding cancellation to building: " + message);
                        channel.basicPublish("building_exchange", building, true, props, message.getBytes(StandardCharsets.UTF_8));
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                    found = true;
                    break;
                }
            }
        }

        if (!found) {
            replyImmediately(replyTo, correlationId, "Reservation not found");
        }
    }

    /**
     * Finalize the cancellation and respond to the customer
     *
     * @param content the content of the message sent from building
     */
    private void finalizeCancellation(String content) {
        String[] parts = content.split(",");

        String message = parts[0];
        String reservationId = parts[1];
        String correlationId = parts[2];
        String customerQueue = parts[3];

        try {
            AMQP.BasicProperties props = getReplyProps(correlationId);
            String response = message + " " + reservationId;

            System.out.println("[x] Responding to customer: " + response);
            channel.basicPublish("", customerQueue, props, response.getBytes(StandardCharsets.UTF_8));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
