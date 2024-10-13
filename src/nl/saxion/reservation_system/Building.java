package nl.saxion.reservation_system;

import com.rabbitmq.client.Channel;

import java.util.HashMap;
import java.util.Map;

public class Building {
    private final String name;
    private final HashMap<String, Boolean> conferenceRooms;
    public static final long HEARTBEAT_INTERVAL = 5000;

    public static void main(String[] args) {
        new Building(java.util.UUID.randomUUID().toString().substring(0, 8)).run();
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

    public boolean isRoomAvailable(String roomName) {
        return conferenceRooms.get(roomName);
    }

    public String bookRoom(String roomName){
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

    public void run() {

    }
}
