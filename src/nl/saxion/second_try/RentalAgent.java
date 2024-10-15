package nl.saxion.second_try;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.*;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeoutException;

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

    private final long BUILDING_TIMEOUT = 5000;
    private String uniqueQueueName;
    private Channel channel;

    public void run() throws IOException, TimeoutException {
        setUpRabbitMQ();
    }

    // Each agent has a unique name, which is used to create a queue. This queue is bind to different exchanges.
    // Message are sent to the exchange, and the agent listens to the queue.
    private void setUpRabbitMQ() throws IOException, TimeoutException {
        // 1. Connect
        initializeConnection();
        // 2. Consume messages and process them
        consumeMessages();
    }

    private void initializeConnection() throws IOException, TimeoutException {
        ConnectionFactory factory = new ConnectionFactory();
        Connection connection = factory.newConnection();
        this.channel = connection.createChannel();
    }

    private void consumeMessages() throws IOException {
        // 1. Create a queues
        // 2. Bind the queue to the exchanges
        // 3. Consume messages
        channel.basicQos(1);

        // uniqueQueueName is used to create a unique queue for each agent
        uniqueQueueName = channel.queueDeclare().getQueue();

        //Exchanges: Heartbeat, Building availability, Customer requests (Cancel, Make reservation)
        channel.exchangeDeclare("heartbeat_exchange", BuiltinExchangeType.FANOUT);
        channel.exchangeDeclare("building_exchange", BuiltinExchangeType.DIRECT);
        channel.exchangeDeclare("customer_exchange", BuiltinExchangeType.DIRECT);

        //Bind queues;
        channel.queueDeclare("rental_agent_queue", false, false, false, null);
        channel.queueBind(uniqueQueueName, "heartbeat_exchange", "");
        channel.queueBind(uniqueQueueName, "building_exchange", "rental_agent_queue");
        channel.queueBind("rental_agent_queue", "customer_exchange", "rental_agent_queue");

        //Consume both unique and work queue
        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            String message = new String(delivery.getBody(), StandardCharsets.UTF_8);
            processMessage(message.split("/"), delivery);
        };

        channel.basicConsume(uniqueQueueName, true, deliverCallback, consumerTag -> {
        });

        channel.basicConsume("rental_agent_queue", true, deliverCallback, consumerTag -> {
        });
    }

    private void processMessage(String[] messageParts, Delivery delivery) {
        // 1. Parse the message
        // 2. Process the message
        // 3. Send a response
        String sender = messageParts[0].toLowerCase();
        String messageType = messageParts[1].toLowerCase();
        String content = messageParts.length > 2 ? messageParts[2] : "";

        System.out.println("[x] Received message from " + sender + ": " + content);

        switch (sender) {
            case "building" -> {
                //Process building message
                switch (messageType) {
                    case "heartbeat" -> {
                        //Process heartbeat message
                        updateBuildings(content);
                    }
                    case "reservation_response" -> {
                        // Process availability message
                        // Response to a reserve request with
                        // a reservation number or a failure message
                        processReservationResponse(content);
                    }
                    case "reservation_finalized" -> {
                        // Process finalized reservation
                        // Update the reservation status to finalized
                        // Respond to the customer with the reservation number
                        System.out.println("Finalized reservation: " + content);
                        finalizeReservation(content);
                    }
                    case "reservation_cancelled" -> {
                        // Process cancelled reservation
                        // Remove the reservation
                        // Send a confirmation to the customer
                        finalizeCancellation(content);
                    }
                }
            }
            case "customer" -> {
                //Process customer message
                switch (messageType) {
                    case "request_rooms" -> {
                        // reply with available rooms
                        sendAvailableRooms(delivery);
                    }
                    case "make_reservation" -> {
                        //Process reservation message
                        // check if building and room exists in knownBuildings
                        // query building for room availability
                        // send reservation number or failure message returned by building
                        queryBuildingAvailability(content, delivery);
                    }
                    case "confirm_reservation" -> {
                        // Process confirmation:
                        // Check local reservation list and find the respective building
                        // if building exists, forward confirmation to building
                        // If building doesn't exist or died in the meantime, respond to customer
                        processConfirmation(content, delivery);
                    }
                    case "cancel_reservation" -> {
                        //Process cancel message
                        // Forward request to building and wait for response
                        // Forward response to customer
                        processCancellation(content, delivery);
                    }
                }
            }

        }
    }

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

    private void sendAvailableRooms(Delivery delivery) {
        try {
            String correlationId = delivery.getProperties().getCorrelationId();
            String customerQueue = delivery.getProperties().getReplyTo();
            String response = getAvailableRoomsJson();

            response = response.isEmpty() ? "No rooms available" : response;

            AMQP.BasicProperties props = new AMQP.BasicProperties.Builder()
                    .correlationId(correlationId)
                    .build();

            channel.basicPublish("", customerQueue, props, response.getBytes(StandardCharsets.UTF_8));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

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

    private void queryBuildingAvailability(String content, Delivery delivery) {
        // Check if building and room exists in knownBuildings
        // If building exists, query building for room availability
        // Otherwise respond to customer immediately
        String building = content.split(",")[0];
        String room = content.split(",")[1];

        if (knownBuildings.containsKey(building) && knownBuildings.get(building).containsKey(room)) {
            // check if room number corresponds to a room in the building
            try {
                String correlationId = delivery.getProperties().getCorrelationId();
                AMQP.BasicProperties props = new AMQP.BasicProperties.Builder()
                        .correlationId(correlationId)
                        .replyTo(uniqueQueueName)
                        .build();

                String message = "rental_agent/reservation_request/" + building + "," + room + "," + correlationId + "," + delivery.getProperties().getReplyTo();

                System.out.println("[x] Forwarding reservation request to building: " + message);
                channel.basicPublish("building_exchange", building, true, props, message.getBytes(StandardCharsets.UTF_8));
            } catch (IOException e) {
                e.printStackTrace();
            }
        } else {
            try {
                String correlationId = delivery.getProperties().getCorrelationId();
                String customerQueue = delivery.getProperties().getReplyTo();
                String response = "Building or room not found";

                AMQP.BasicProperties props = new AMQP.BasicProperties.Builder()
                        .correlationId(correlationId)
                        .build();

                channel.basicPublish("", customerQueue, props, response.getBytes(StandardCharsets.UTF_8));
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    private void processReservationResponse(String content) {
        try {
            String[] parts = content.split(",");
            String building = parts[0];
            String room = parts[1];
            String response = parts[2];
            String correlationId = parts[3];
            String customerQueue = parts[4];

            //Building | Room / Reservation
            if (response.contains("Confirmed")) {
                String reservationId = response.split(" ")[1];
                reservations.computeIfAbsent(building, k -> new ConcurrentHashMap<>())
                        .put(room, new Reservation(reservationId));
            }

            AMQP.BasicProperties props = new AMQP.BasicProperties.Builder()
                    .correlationId(correlationId)
                    .build();

            System.out.println("[x] Forwarding response to customer: " + content);
            channel.basicPublish("", customerQueue, props, response.getBytes(StandardCharsets.UTF_8));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void processConfirmation(String content, Delivery delivery){
        // Check local reservation list and find the respective building
        // if building exists, forward confirmation to building
        // If building doesn't exist or died in the meantime, respond to customer
        String reservationId = content;
        String replyTo = delivery.getProperties().getReplyTo();
        String correlationId = delivery.getProperties().getCorrelationId();

        //DEBUGGING:
        System.out.println("Reservation: " + reservationId);
        System.out.println("Reply to: " + replyTo);
        System.out.println("Correlation ID: " + correlationId);
        System.out.println("Reservations: " + reservations);

        // Loop through each building and check each rooms reservation
        // Reservations: {Building_8a18eee0={room_0=Reservation{id='7f065ef1', finalized=false}}}
        boolean found = false;
        for (String building:reservations.keySet()){
            if (found){
                break;
            }

            for (String room:reservations.get(building).keySet()){
                if (reservations.get(building).get(room).getId().equals(reservationId)){
                    try {
                        AMQP.BasicProperties props = new AMQP.BasicProperties.Builder()
                                .correlationId(correlationId)
                                .replyTo(uniqueQueueName)
                                .build();

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

        if(!found){
            try {
                AMQP.BasicProperties props = new AMQP.BasicProperties.Builder()
                        .correlationId(correlationId)
                        .build();

                String response = "Reservation not found";
                System.out.println("[x] Responding to customer: " + response);
                channel.basicPublish("", replyTo, props, response.getBytes(StandardCharsets.UTF_8));
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    private void finalizeReservation(String content){
        // Process finalized reservation
        // Update the reservation status to finalized
        // Respond to the customer with the reservation number
        String message = content.split(",")[0];
        String reservationId = content.split(",")[1];
        String correlationId = content.split(",")[2];
        String customerQueue = content.split(",")[3];

        try {
            AMQP.BasicProperties props = new AMQP.BasicProperties.Builder()
                    .correlationId(correlationId)
                    .build();

            String response = message + ": " + reservationId;
            System.out.println("[x] Responding to customer: " + response);
            channel.basicPublish("", customerQueue, props, response.getBytes(StandardCharsets.UTF_8));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void processCancellation(String content, Delivery delivery){
        // Check if the reservation exists
        // Loop through each building and check each rooms reservation
        // If found forward the cancellation to the building
        // If not, respond to the customer

        String reservationId = content.split(",")[0];
        String replyTo = delivery.getProperties().getReplyTo();
        String correlationId = delivery.getProperties().getCorrelationId();

        boolean found = false;

        for (String building:reservations.keySet()){
            if (found){
                break;
            }

            for (String room:reservations.get(building).keySet()){
                if (reservations.get(building).get(room).getId().equals(reservationId)){
                    try {
                        AMQP.BasicProperties props = new AMQP.BasicProperties.Builder()
                                .correlationId(correlationId)
                                .replyTo(uniqueQueueName)
                                .build();

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

        if (!found){
            try {
                AMQP.BasicProperties props = new AMQP.BasicProperties.Builder()
                        .correlationId(correlationId)
                        .build();

                String response = "Reservation not found";
                System.out.println("[x] Responding to customer: " + response);
                channel.basicPublish("", replyTo, props, response.getBytes(StandardCharsets.UTF_8));
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

    }

    private void finalizeCancellation(String content){
        System.out.println("Cancellation: " + content);

        String[] parts = content.split(",");

        String message = parts[0];
        String reservationId = parts[1];
        String correlationId = parts[2];
        String customerQueue = parts[3];

        try {
            AMQP.BasicProperties props = new AMQP.BasicProperties.Builder()
                    .correlationId(correlationId)
                    .build();

            String response = message + ": " + reservationId;
            System.out.println("[x] Responding to customer: " + response);
            channel.basicPublish("", customerQueue, props, response.getBytes(StandardCharsets.UTF_8));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
