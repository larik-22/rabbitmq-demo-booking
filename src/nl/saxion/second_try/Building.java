package nl.saxion.second_try;

import com.fasterxml.jackson.core.JsonProcessingException;
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

    public static final long HEARTBEAT_INTERVAL = 1000;
    private final String name;
    private final HashMap<String, Boolean> conferenceRooms;
    private final Map<String, Reservation> reservations = new ConcurrentHashMap<>(); // room, reservation
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
            processMessage(message.split("/"), delivery);
        }, consumerTag -> {
        });
    }

    private void processMessage(String[] messageParts, Delivery delivery) {
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
            case "confirm_reservation" -> {
                // Update the reservation status
                // Send a confirmation to the customer
                // [x] Received message: 0fd4ad38,fe2dfc9b-ff76-497c-a0ff-2d49cdc34b30,Customer 0641976c
                confirmReservation(content, delivery);
            }
            case "cancel_reservation" -> {
                // Remove the reservation
                // Send a confirmation to the customer
                System.out.println("Received cancel reservation request");
                cancelReservation(content, delivery);
            }
        }
    }

    private void processReservation(String content, Delivery delivery) {
        String room = content.split(",")[1];
        String correlationId = content.split(",")[2];
        String customerQueue = content.split(",")[3];

        String message;
        if (conferenceRooms.get(room)){
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

    private void confirmReservation(String content, Delivery delivery){
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

    private void cancelReservation(String content, Delivery delivery){
        String reservationId = content.split(",")[0];
        String room = content.split(",")[1];
        String correlationId = content.split(",")[2];
        String customerQueue = content.split(",")[3];

        reservations.remove(room);
        if(conferenceRooms.containsKey(room)){
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

    private HashMap<String, Boolean> generateRandomRooms(int amount) {
        HashMap<String, Boolean> rooms = new HashMap<>();
        for (int i = 0; i < amount; i++) {
            rooms.put("room_" + i, true);
        }

        return rooms;
    }

    private void sendHeartbeat(Channel channel) {
        Timer timer = new Timer();
        timer.schedule(new HeartbeatTask(channel), 0, HEARTBEAT_INTERVAL);
    }

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
