import java.io.*;
import java.net.*;
import java.util.Arrays;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class Server {
    private static final ConcurrentHashMap<String, String> tupleSpace = new ConcurrentHashMap<>();
    private static final AtomicInteger totalClients = new AtomicInteger(0);
    private static final AtomicInteger totalOperations = new AtomicInteger(0);
    private static final AtomicInteger totalPuts = new AtomicInteger(0);
    private static final AtomicInteger totalGets = new AtomicInteger(0);
    private static final AtomicInteger totalReads = new AtomicInteger(0);
    private static final AtomicInteger totalErrors = new AtomicInteger(0);
    private static final AtomicLong totalKeyLength = new AtomicLong(0);
    private static final AtomicLong totalValueLength = new AtomicLong(0);

    public static void main(String[] args) throws IOException {
        // 添加参数验证
        if (args.length != 1) {
            System.err.println("Usage: java Server <port>");
            System.exit(1);
        }

        try {
            int port = Integer.parseInt(args[0]);
            // 剩余代码保持不变...
        } catch (NumberFormatException e) {
            System.err.println("Invalid port number: " + args[0]);
            System.exit(2);
        }

        int port = Integer.parseInt(args[0]);
        try (ServerSocket serverSocket = new ServerSocket(port)) {
            System.out.println("Server started on port " + port);

            // 每10秒输出统计信息
            ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
            scheduler.scheduleAtFixedRate(Server::printStats, 0, 10, TimeUnit.SECONDS);

            while (true) {
                Socket clientSocket = serverSocket.accept();
                new Thread(new ClientHandler(clientSocket)).start();
                totalClients.incrementAndGet();
            }
        }
    }

    private static void printStats() {
        int size = tupleSpace.size();
        long avgTupleSize = 0;
        long avgKeySize = 0;
        long avgValueSize = 0;
        if (size > 0) {
            long totalKey = totalKeyLength.get();
            long totalValue = totalValueLength.get();
            avgKeySize = totalKey / size;
            avgValueSize = totalValue / size;
            avgTupleSize = (totalKey + totalValue) / size;
        }
        System.out.printf("[Server Stats] Tuples: %d, Avg Tuple Size: %d, Avg Key Size: %d, Avg Value Size: %d, Total Clients: %d, Total Ops: %d, PUTs: %d, GETs: %d, READs: %d, Errors: %d%n",
                size,
                avgTupleSize,
                avgKeySize,
                avgValueSize,
                totalClients.get(),
                totalOperations.get(),
                totalPuts.get(),
                totalGets.get(),
                totalReads.get(),
                totalErrors.get());
    }

    static class ClientHandler implements Runnable {
        private final Socket clientSocket;

        public ClientHandler(Socket socket) {
            this.clientSocket = socket;
        }

        @Override
        public void run() {
            try (BufferedReader in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
                 PrintWriter out = new PrintWriter(clientSocket.getOutputStream(), true)) {

                String request;
                while ((request = in.readLine()) != null) {
                    totalOperations.incrementAndGet();
                    String response = processRequest(request);
                    out.println(response);
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        private String processRequest(String request) {
            String[] parts = request.split(" ");
            if (parts.length < 3) {
                totalErrors.incrementAndGet();
                return "ERR invalid request format";
            }
            String op = parts[1];
            String key = parts[2];
            String value = (parts.length > 3) ? String.join(" ", Arrays.copyOfRange(parts, 3, parts.length)) : null;

            switch (op) {
                case "P":
                    if (tupleSpace.containsKey(key)) {
                        totalErrors.incrementAndGet();
                        return String.format("ERR %s already exists", key);
                    } else {
                        tupleSpace.put(key, value);
                        totalKeyLength.addAndGet(key.length());
                        totalValueLength.addAndGet(value != null ? value.length() : 0);
                        totalPuts.incrementAndGet();
                        return String.format("OK (%s, %s) added", key, value);
                    }
                case "G":
                    if (tupleSpace.containsKey(key)) {
                        String val = tupleSpace.remove(key);
                        int keyLen = key.length();
                        int valLen = val != null ? val.length() : 0;
                        totalKeyLength.addAndGet(-keyLen);
                        totalValueLength.addAndGet(-valLen);
                        totalGets.incrementAndGet();
                        return String.format("OK (%s, %s) removed", key, val);
                    } else {
                        totalErrors.incrementAndGet();
                        return String.format("ERR %s does not exist", key);
                    }
                case "R":
                    if (tupleSpace.containsKey(key)) {
                        totalReads.incrementAndGet();
                        return String.format("OK (%s, %s) read", key, tupleSpace.get(key));
                    } else {
                        totalErrors.incrementAndGet();
                        return String.format("ERR %s does not exist", key);
                    }
                default:
                    totalErrors.incrementAndGet();
                    return "ERR invalid operation";
            }
        }
    }
}