package nl.saxion.reservation_system;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.*;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.TimeoutException;

public class Building {
    public static final long HEARTBEAT_INTERVAL = 1000;
    private static final String RENTAL_AGENT_EXCHANGE = "building_to_rental_agent";
    private static final String BUILDING_AVAILABILITY_EXCHANGE = "building_availability_exchange";
    private final String heartbeatExchange = "building_heartbeat_exchange";

    private Channel channel;
    private final String name;
    private final HashMap<String, Boolean> conferenceRooms;
    private final HashMap<String, String> reservations = new HashMap<>();

    public static void main(String[] args) throws IOException, TimeoutException {
        new Building("Building " + UUID.randomUUID().toString().substring(0, 8)).run();
    }

    public Building(String name) {
        this.name = name;
        this.conferenceRooms = generateRandomRooms(3);
    }

    public HashMap<String, Boolean> generateRandomRooms(int amount) {
        HashMap<String, Boolean> rooms = new HashMap<>();
        for (int i = 0; i < amount; i++) {
            rooms.put("Room " + i, true);
        }

        return rooms;
    }

    public void run() throws IOException, TimeoutException {
        connectToRabbitMQ();
    }

    public void connectToRabbitMQ() throws IOException, TimeoutException {
        ConnectionFactory factory = new ConnectionFactory();
        Connection connection = factory.newConnection();
        this.channel = connection.createChannel();

        sendHeartbeat(channel);
        setupReservationRequestListener();
    }

    private void setupReservationRequestListener() throws IOException {
        channel.exchangeDeclare(RENTAL_AGENT_EXCHANGE, BuiltinExchangeType.DIRECT);
        String queue = channel.queueDeclare(name, false, false, false, null).getQueue();
        channel.queueBind(queue, RENTAL_AGENT_EXCHANGE, name);

        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            String message = new String(delivery.getBody(), StandardCharsets.UTF_8);
            System.out.println("Received reservation request: " + message);
            ObjectMapper objectMapper = new ObjectMapper();
            Map<String, String> request = objectMapper.readValue(message, Map.class);

            // Check if the room is available
            String roomName = request.get("roomName");
            if (isRoomAvailable(roomName)) {
                String reservationId = bookRoom(roomName);

            } else {
                // Send a message to the customer that the room is unavailable

            }
        };

        channel.basicConsume(queue, true, deliverCallback, consumerTag -> {
        });
    }

    public boolean isRoomAvailable(String roomName) {
        return conferenceRooms.get(roomName);
    }

    public String bookRoom(String roomName) {
        conferenceRooms.put(roomName, false);
        return UUID.randomUUID().toString().substring(0, 8);
    }

    public boolean cancelReservation(String roomName) {
        return false;
    }

    public void sendHeartbeat(Channel channel) {
        Timer timer = new Timer();
        timer.schedule(new HeartbeatTask(channel, name, conferenceRooms), 0, HEARTBEAT_INTERVAL);
    }

    private class HeartbeatTask extends TimerTask {
        private final Channel channel;
        private final String buildingName;
        private final HashMap<String, Boolean> rooms;

        public HeartbeatTask(Channel channel, String buildingName, HashMap<String, Boolean> rooms) {
            this.channel = channel;
            this.buildingName = buildingName;
            this.rooms = rooms;
        }

        @Override
        public void run() {
            try {
                Map<String, Object> message = new HashMap<>();
                message.put("building", buildingName);
                message.put("rooms", rooms);
                message.put("timestamp", System.currentTimeMillis());

                ObjectMapper objectMapper = new ObjectMapper();
                String jsonMessage = objectMapper.writeValueAsString(message);

                System.out.println("Sending heartbeat: " + jsonMessage);
                channel.exchangeDeclare(heartbeatExchange, BuiltinExchangeType.FANOUT);
                channel.basicPublish(heartbeatExchange, "", null, jsonMessage.getBytes(StandardCharsets.UTF_8));
            } catch (Exception e) {
                throw new RuntimeException("Failed to send heartbeat", e);
            }
        }
    }
}
