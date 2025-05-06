package Server;

import java.io.*;
import java.net.*;
import java.util.concurrent.*;
import java.util.*;

public class Server {
    private static final ConcurrentHashMap<String, String> tupleSpace = new ConcurrentHashMap<>();
    private static final Stats stats = new Stats();
    private static ScheduledExecutorService timer = Executors.newScheduledThreadPool(1);

    public static void main(String[] args) {
        if (args.length != 1) {
            System.err.println("Usage: java Server <port>");
            return;
        }

        int port = Integer.parseInt(args[0]);
        timer.scheduleAtFixedRate(Server::printStats, 10, 10, TimeUnit.SECONDS);

        try (ServerSocket serverSocket = new ServerSocket(port)) {
            System.out.println("Server started on port " + port);
            while (true) {
                Socket clientSocket = serverSocket.accept();
                stats.incrementClients();
                new Thread(() -> handleClient(clientSocket)).start();
            }
        } catch (IOException e) {
            System.err.println("Server error: " + e.getMessage());
        } finally {
            timer.shutdown();
        }
    }

    private static void handleClient(Socket clientSocket) {
        try (BufferedReader in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
             PrintWriter out = new PrintWriter(clientSocket.getOutputStream(), true)) {

            String inputLine;
            while ((inputLine = in.readLine()) != null) {
                if (inputLine.length() < 4) {
                    sendError(out, "Invalid message format");
                    continue;
                }

                int length;
                try {
                    length = Integer.parseInt(inputLine.substring(0, 3));
                } catch (NumberFormatException e) {
                    sendError(out, "Invalid length prefix");
                    continue;
                }

                String payload = inputLine.substring(4);
                String[] parts = payload.split("\\s+", 3);
                if (parts.length < 2) {
                    sendError(out, "Malformed request");
                    continue;
                }

                String command = parts[0];
                String key = parts[1];
                String value = (parts.length > 2) ? parts[2] : "";

                String response;
                switch (command) {
                    case "P":
                        stats.incrementPuts();
                        if (tupleSpace.containsKey(key)) {
                            stats.incrementErrors();
                            response = "ERR " + key + " already exists";
                        } else {
                            tupleSpace.put(key, value);
                            response = "OK (" + key + ", " + value + ") added";
                        }
                        break;
                    case "R":
                        stats.incrementReads();
                        if (tupleSpace.containsKey(key)) {
                            response = "OK (" + key + ", " + tupleSpace.get(key) + ") read";
                        } else {
                            stats.incrementErrors();
                            response = "ERR " + key + " does not exist";
                        }
                        break;
                    case "G":
                        stats.incrementGets();
                        if (tupleSpace.containsKey(key)) {
                            String val = tupleSpace.remove(key);
                            response = "OK (" + key + ", " + val + ") removed";
                        } else {
                            stats.incrementErrors();
                            response = "ERR " + key + " does not exist";
                        }
                        break;
                    default:
                        response = "ERR Unknown command";
                }

                String formattedResponse = String.format("%03d %s", response.length() + 4, response);
                out.println(formattedResponse);
            }
        } catch (IOException e) {
            System.err.println("Client handler error: " + e.getMessage());
        } finally {
            stats.decrementClients();
        }
    }

    private static void sendError(PrintWriter out, String message) {
        String response = "ERR " + message;
        out.println(String.format("%03d %s", response.length() + 4, response));
    }

    private static void printStats() {
        int tupleCount = tupleSpace.size();
        double avgKeyLen = tupleSpace.keySet().stream().mapToInt(String::length).average().orElse(0);
        double avgValLen = tupleSpace.values().stream().mapToInt(String::length).average().orElse(0);

        System.out.printf("[STATS] Tuples: %d | Avg Key: %.1f | Avg Val: %.1f | Clients: %d | PUTs: %d | GETs: %d | ERRORS: %d%n",
                tupleCount, avgKeyLen, avgValLen, stats.getClients(), stats.getPuts(), stats.getGets(), stats.getErrors());
    }

    static class Stats {
        private int clients = 0;
        private int puts = 0;
        private int gets = 0;
        private int reads = 0;
        private int errors = 0;

        public synchronized void incrementClients() { clients++; }
        public synchronized void decrementClients() { clients--; }
        public synchronized void incrementPuts() { puts++; }
        public synchronized void incrementGets() { gets++; }
        public synchronized void incrementReads() { reads++; }
        public synchronized void incrementErrors() { errors++; }

        public synchronized int getClients() { return clients; }
        public synchronized int getPuts() { return puts; }
        public synchronized int getGets() { return gets; }
        public synchronized int getReads() { return reads; }
        public synchronized int getErrors() { return errors; }
    }
}