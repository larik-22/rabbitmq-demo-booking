package nl.saxion.paracomp;

public class Consumers {
    public static void main(String[] args) {
        Thread consumer1 = new Thread(new Consumer("Ben"));
        Thread consumer2 = new Thread(new Consumer("Yasha"));

        consumer1.start();
        consumer2.start();

    }
}
