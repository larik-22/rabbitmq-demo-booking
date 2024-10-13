package nl.saxion.reservation_system;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ConnectionFactory;

import java.util.List;
import java.util.Scanner;

public class Customer {
    public static void main(String[] args) {
        new Customer().run();
    }

    private static final List<String> MENU = List.of("View available rooms", "Make a reservation", "Cancel reservation", "Quit");
    private static final Scanner scanner = new Scanner(System.in);
    private Channel channel;

    public void run(){
        initializeConnection();

        outer:
        while (true) {
            System.out.println("Please select an option:");
            for (int i = 0; i < MENU.size(); i++) {
                System.out.println(i + 1 + ". " + MENU.get(i));
            }
            System.out.println("Enter your choice:");
            int choice = scanner.nextInt();
            switch (choice) {
                case 1:
//                    viewAvailableRooms();
                    break;
                case 2:
//                    makeReservation();
                    break;
                case 3:
//                    cancelReservation();
                    break;
                case 4:
                    System.out.println("Goodbye!");
                    return;
                default:
                    System.out.println("Invalid choice, please try again.");
            }
        }
    }

    private void initializeConnection(){
        try {
            ConnectionFactory factory = new ConnectionFactory();
            factory.setHost("localhost");
            channel = factory.newConnection().createChannel();
        } catch (Exception e) {
            throw new RuntimeException("Failed to connect to RabbitMQ", e);
        }
    }
}
