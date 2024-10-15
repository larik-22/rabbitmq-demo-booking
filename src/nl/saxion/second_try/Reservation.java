package nl.saxion.second_try;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class Reservation {
    private String id;
    private boolean finalized;

    public Reservation(String id, boolean finalized) {
        if (id == null || id.isBlank()) {
            throw new IllegalArgumentException("Reservation ID cannot be null or empty");
        }
        this.id = id;
        this.finalized = finalized;
    }

    public Reservation(String id) {
        this(id, false);
    }

    public String getId() {
        return id;
    }

    public boolean isFinalized() {
        return finalized;
    }

    public void setFinalized(boolean finalized) {
        this.finalized = finalized;
    }

    public static Map<String, Reservation> fromJson(String json) throws IOException {
        ObjectMapper objectMapper = new ObjectMapper();
        Map<String, Map<String, Object>> reservationsMap = objectMapper.readValue(json, Map.class);
        Map<String, Reservation> reservations = new HashMap<>();

        for (Map.Entry<String, Map<String, Object>> entry : reservationsMap.entrySet()) {
            String room = entry.getKey();
            Map<String, Object> reservationData = entry.getValue();
            String reservationId = (String) reservationData.get("id");
            boolean finalized = (boolean) reservationData.get("finalized");
            reservations.put(room, new Reservation(reservationId, finalized));
        }

        return reservations;
    }

    @Override
    public String toString() {
        return "Reservation{" +
                "id='" + id + '\'' +
                ", finalized=" + finalized +
                '}';
    }
}
