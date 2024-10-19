package nl.saxion.second_try;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.*;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeoutException;

public class Building {
    public static void main(String[] args) throws IOException, TimeoutException {
        new Building("Building_" + UUID.randomUUID().toString().substring(0, 8)).run();
    }

    // The interval in which the building sends heartbeats to the agents
    public static final long HEARTBEAT_INTERVAL = 1000;
    private final String name;
    private final HashMap<String, Boolean> conferenceRooms;
    private final Map<String, Reservation> reservations = new ConcurrentHashMap<>(); // room, reservation
    private Channel channel;

    public Building(String name) {
        this.name = name;
        this.conferenceRooms = generateRandomRooms(3);
    }

    public void run() throws IOException, TimeoutException {
        setUpRabbitMQ();
    }

    /**
     * Sets up the RabbitMQ logic
     * Connects to the server, starts listening for messages and starts sending heartbeats
     */
    private void setUpRabbitMQ() throws IOException, TimeoutException {
        // 1. Connect
        // 2. Setup exchanges and queues
        // 3. Send heartbeats
        // 4. Listen for customer requests
        initializeConnection();
        startListeningForMessages();
        sendHeartbeat(channel);
    }

    /**
     * Initializes the connection to the RabbitMQ server
     */
    private void initializeConnection() throws IOException, TimeoutException {
        ConnectionFactory factory = new ConnectionFactory();
        Connection connection = factory.newConnection();
        this.channel = connection.createChannel();
    }

    /**
     * Declares exchanges and unique building queue
     * and starts listening for upcoming messages
     */
    private void startListeningForMessages() throws IOException {
        String buildingQueue = channel.queueDeclare().getQueue();
        channel.exchangeDeclare("heartbeat_exchange", BuiltinExchangeType.FANOUT);
        channel.exchangeDeclare("building_exchange", BuiltinExchangeType.DIRECT);
        channel.queueBind(buildingQueue, "building_exchange", name);

        channel.basicConsume(buildingQueue, true, (consumerTag, delivery) -> {
            String message = new String(delivery.getBody(), StandardCharsets.UTF_8);
            processMessage(message.split("/"), delivery);
        }, consumerTag -> {
        });
    }

    /**
     * Main method to process all consumed messages
     * Follows the pattern of the message: [type]/[content], since we always know the sender (rental agent)
     *
     * @param messageParts the parts of the message, split by "/"
     * @param delivery     the delivery object of the message containing replyTo and correlationId of the rental agent
     */
    private void processMessage(String[] messageParts, Delivery delivery) {
        String messageType = messageParts[1].toLowerCase();
        String content = messageParts.length > 2 ? messageParts[2] : "";

        //always only agent
        System.out.println("[x] Received message" + ": " + content);

        switch (messageType) {
            case "reservation_request" -> processReservation(content, delivery);
            case "confirm_reservation" -> confirmReservation(content, delivery);
            case "cancel_reservation" -> cancelReservation(content, delivery);
        }
    }

    /**
     * Processes a reservation request and responds to the rental agent with the reservation number
     * Or with a message that the room is not available
     *
     * @param content  the content of the message
     * @param delivery the delivery object of the message containing replyTo and correlationId of the rental agent
     */
    private void processReservation(String content, Delivery delivery) {
        String room = content.split(",")[1];
        String correlationId = content.split(",")[2];
        String customerQueue = content.split(",")[3];

        String message;
        if (conferenceRooms.get(room)) {
            message = UUID.randomUUID().toString().substring(0, 8);
            conferenceRooms.put(room, false);
            reservations.put(room, new Reservation(message));

            message = "ReservationNr " + message;
        } else {
            message = "Room is not available";
        }

        String response = "building/reservation_response/" + name + "," + room + "," + message + "," + correlationId + "," + customerQueue;

        try {
            new HeartbeatTask(channel).run();
            channel.basicPublish("", delivery.getProperties().getReplyTo(), null, response.getBytes(StandardCharsets.UTF_8));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Confirms a reservation and replies to the rental agent with the confirmation
     *
     * @param content  the content of the message
     * @param delivery the delivery object of the message containing replyTo and correlationId of the rental agent
     */
    private void confirmReservation(String content, Delivery delivery) {
        String reservationId = content.split(",")[0];
        String correlationId = content.split(",")[1];
        String customerQueue = content.split(",")[2];

        // Finalize the reservation
        for (String room : reservations.keySet()) {
            if (reservations.get(room).getId().equals(reservationId)) {
                reservations.get(room).setFinalized(true);
            }
        }

        // respond to the rental agent first
        String response = "building/reservation_finalized/" + "Reservation Finalized" + "," + reservationId + "," + correlationId + "," + customerQueue;
        try {
            // Immediately send a heartbeat to all the agents
            new HeartbeatTask(channel).run();
            channel.basicPublish("", delivery.getProperties().getReplyTo(), null, response.getBytes(StandardCharsets.UTF_8));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Cancels a reservation. The room is made available again
     * Answers back to the rental agent that made the request
     *
     * @param content  the content of the message
     * @param delivery the delivery object of the message containing replyTo and correlationId of the rental agent
     */
    private void cancelReservation(String content, Delivery delivery) {
        String reservationId = content.split(",")[0];
        String room = content.split(",")[1];
        String correlationId = content.split(",")[2];
        String customerQueue = content.split(",")[3];

        reservations.remove(room);
        if (conferenceRooms.containsKey(room)) {
            conferenceRooms.put(room, true);
        }

        String response = "building/reservation_cancelled/" + "Reservation Cancelled" + "," + reservationId + "," + correlationId + "," + customerQueue;
        try {
            new HeartbeatTask(channel).run();
            channel.basicPublish("", delivery.getProperties().getReplyTo(), null, response.getBytes(StandardCharsets.UTF_8));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Generates a random amount of free rooms
     *
     * @param amount the amount of rooms to generate
     * @return a hashmap with the room names as keys and a boolean indicating if the room is free
     */
    private HashMap<String, Boolean> generateRandomRooms(int amount) {
        HashMap<String, Boolean> rooms = new HashMap<>();
        for (int i = 0; i < amount; i++) {
            rooms.put("room_" + i, true);
        }

        return rooms;
    }

    /**
     * Sends a heartbeat to the agents
     *
     * @param channel
     */
    private void sendHeartbeat(Channel channel) {
        Timer timer = new Timer();
        timer.schedule(new HeartbeatTask(channel), 0, HEARTBEAT_INTERVAL);
    }

    /**
     * A task that sends a heartbeat to the agents
     * Including the building name, the conference rooms, the reservations and the timestamp
     */
    private class HeartbeatTask extends TimerTask {
        private final Channel channel;

        public HeartbeatTask(Channel channel) {
            this.channel = channel;
        }

        @Override
        public void run() {
            try {
                Map<String, Object> message = new HashMap<>();
                message.put("building", name);
                message.put("rooms", conferenceRooms);
                message.put("timestamp", System.currentTimeMillis());
                message.put("reservations", reservations);

                ObjectMapper objectMapper = new ObjectMapper();
                String jsonMessage = "building/heartbeat/" + objectMapper.writeValueAsString(message);

                //TODO: uncomment to debug
                System.out.println("Sending heartbeat: " + jsonMessage);
                channel.basicPublish("heartbeat_exchange", "", null, jsonMessage.getBytes(StandardCharsets.UTF_8));
            } catch (Exception e) {
                throw new RuntimeException("Failed to send heartbeat", e);
            }
        }
    }
}
