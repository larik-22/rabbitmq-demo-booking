package nl.saxion.reservation_system;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.*;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.TimeoutException;

public class Building {
    private Channel channel;
    public static final long HEARTBEAT_INTERVAL = 1000;
    private static final String RENTAL_AGENT_EXCHANGE = "building_to_rental_agent";
    private final String heartbeatExchange = "building_heartbeat_exchange";

    private final String name;
    private final HashMap<String, Boolean> conferenceRooms;
    private final HashMap<String, String> reservations = new HashMap<>();

    public static void main(String[] args) throws IOException, TimeoutException {
        new Building("Building " + java.util.UUID.randomUUID().toString().substring(0, 8)).run();
    }

    public Building(String name) {
        this.name = name;
        this.conferenceRooms = generateRandomRooms(3);
    }

    public void run() throws IOException, TimeoutException {
        connectToRabbitMQ();
    }

    public void connectToRabbitMQ() throws IOException, TimeoutException {
        ConnectionFactory factory = new ConnectionFactory();
        Connection connection = factory.newConnection();
        this.channel = connection.createChannel();

        sendHeartbeat(channel);
    }

    public void sendHeartbeat(Channel channel) {
        Timer timer = new Timer();
        timer.schedule(new HeartbeatTask(channel, name, conferenceRooms), 0, HEARTBEAT_INTERVAL);
    }

    public HashMap<String, Boolean> generateRandomRooms(int amount) {
        HashMap<String, Boolean> rooms = new HashMap<>();
        for (int i = 0; i < amount; i++) {
            rooms.put("Room " + i, true);
        }

        return rooms;
    }

    public boolean isRoomAvailable(String roomName) {
        return conferenceRooms.get(roomName);
    }

    public String bookRoom(String roomName) {
        if (conferenceRooms.getOrDefault(roomName, false)) {
            conferenceRooms.put(roomName, false);
            return java.util.UUID.randomUUID().toString().substring(0, 8); // Generate a unique reservation number
        } else {
            return null;
        }
    }

    public boolean cancelReservation(String roomName) {
        if (conferenceRooms.containsKey(roomName)) {
            conferenceRooms.put(roomName, true);
            return true;
        }
        return false;
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
                channel.basicPublish(heartbeatExchange, "", null, jsonMessage.getBytes(StandardCharsets.UTF_8));
            } catch (Exception e) {
                throw new RuntimeException("Failed to send heartbeat", e);
            }
        }
    }
}
