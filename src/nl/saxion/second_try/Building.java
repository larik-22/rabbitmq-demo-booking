package nl.saxion.second_try;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.*;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.TimeoutException;

public class Building {
    public static void main(String[] args) throws IOException, TimeoutException {new Building("Building_" + UUID.randomUUID().toString().substring(0, 8)).run();}

    public static final long HEARTBEAT_INTERVAL = 2500;
    private final String name;
    private final HashMap<String, Boolean> conferenceRooms;
    private Channel channel;
    private String buildingQueue;

    public Building(String name) {
        this.name = name;
        this.conferenceRooms = generateRandomRooms(3);
    }

    public void run() throws IOException, TimeoutException {
        setUpRabbitMQ();
    }

    private void setUpRabbitMQ() throws IOException, TimeoutException {
        // 1. Connect
        // 2. Setup exchanges and queues
        // 3. Send heartbeats
        // 4. Listen for customer requests

        initializeConnection();
        consumeMessages();
        sendHeartbeat(channel);
    }

    private void initializeConnection() throws IOException, TimeoutException {
        ConnectionFactory factory = new ConnectionFactory();
        Connection connection = factory.newConnection();
        this.channel = connection.createChannel();
    }

    private void consumeMessages() throws IOException {
        // 1. Create a queues and exchanges
        // 2. Bind the queue to the exchanges
        // 3. Consume messages
        // 4. Handle messages
        buildingQueue = channel.queueDeclare().getQueue();
        channel.exchangeDeclare("heartbeat_exchange", BuiltinExchangeType.FANOUT);
        channel.exchangeDeclare("building_exchange", BuiltinExchangeType.DIRECT);
        channel.queueBind(buildingQueue, "building_exchange", name);

        channel.basicConsume(buildingQueue, true, (consumerTag, delivery) -> {
            String message = new String(delivery.getBody(), StandardCharsets.UTF_8);
            System.out.println("Received message: " + message);
            processMessage(message.split("/"), delivery);

        }, consumerTag -> {});
    }

    private void processMessage(String[] messageParts, Delivery delivery){
        String messageType = messageParts[1].toLowerCase();
        String content = messageParts.length > 2 ? messageParts[2] : "";

        //always only agent
        System.out.println("[x] Received message" + ": " + content);

        switch (messageType) {
            case "reservation_request" -> {
                // Check if the room is available
                // Respond back with the availability (reply to the rental agent)
                processReservation(content, delivery);
            }
        }
    }

    private void processReservation(String content, Delivery delivery){
        // If room available, return unique reservation ID and book the room
        // If not available, return "Room not available"
        String room = content.split(",")[1];
        String correlationId = content.split(",")[2];
        String customerQueue = content.split(",")[3];

        String response;
        if(conferenceRooms.get(room) != null && conferenceRooms.get(room)){
            response = "building/reservation_response/" + UUID.randomUUID().toString().substring(0, 8);
            conferenceRooms.put(room, false);
        } else {
            response = "building/reservation_response/Room not available";
        }

        response = response + "," + correlationId + "," + customerQueue;

        // reply to agent
        System.out.println("Sending response: " + response);
        try {
            channel.basicPublish("", delivery.getProperties().getReplyTo(), null, response.getBytes(StandardCharsets.UTF_8));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private HashMap<String, Boolean> generateRandomRooms(int amount) {
        HashMap<String, Boolean> rooms = new HashMap<>();
        for (int i = 0; i < amount; i++) {
            rooms.put("room_" + i, true);
        }

        return rooms;
    }

    private void sendHeartbeat(Channel channel) {
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
                String jsonMessage = "building/heartbeat/" + objectMapper.writeValueAsString(message);

                //TODO: uncomment to debug
//                System.out.println("Sending heartbeat: " + jsonMessage);
                channel.basicPublish("heartbeat_exchange", "", null, jsonMessage.getBytes(StandardCharsets.UTF_8));
            } catch (Exception e) {
                throw new RuntimeException("Failed to send heartbeat", e);
            }
        }
    }
}
