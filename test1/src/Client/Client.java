package Client;

import java.io.*;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class Client {
    public static void main(String[] args) {
        if (args.length != 3) {
            System.err.println("Usage: java Client <hostname> <port> <request_file>");
            return;
        }

        String hostname = args[0];
        int port = Integer.parseInt(args[1]);
        String requestFilePath = args[2];

        // 检查文件是否存在
        Path path = Paths.get(requestFilePath);
        if (!Files.exists(path)) {
            System.err.println("Error: File '" + requestFilePath + "' does not exist.");
            return;
        }

        try (Socket socket = new Socket(hostname, port);
             BufferedReader reader = Files.newBufferedReader(path); // 使用NIO读取文件
             PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
             BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()))) {

            String line;
            while ((line = reader.readLine()) != null) {
                line = line.trim();
                if (line.isEmpty()) continue;

                String[] parts = line.split("\\s+", 3);
                if (parts.length < 2) {
                    System.out.println("Invalid request: " + line);
                    continue;
                }

                String command = parts[0].toUpperCase();
                String key = parts[1];
                String value = (parts.length > 2) ? parts[2] : "";
                String fullString = key + " " + value;

                if (fullString.length() > 970) {
                    System.out.println(line + ": ERR request too long");
                    continue;
                }

                String request;
                switch (command) {
                    case "PUT":
                        if (parts.length < 3) {
                            System.out.println("Invalid PUT format: " + line);
                            continue;
                        }
                        request = String.format("P %s %s", key, value);
                        break;
                    case "READ":
                        request = String.format("R %s", key);
                        break;
                    case "GET":
                        request = String.format("G %s", key);
                        break;
                    default:
                        System.out.println("Unknown command: " + command);
                        continue;
                }

                int length = request.length() + 4;
                if (length > 999) {
                    System.out.println(line + ": ERR request exceeds 999 bytes");
                    continue;
                }

                String formattedRequest = String.format("%03d %s", length, request);
                out.println(formattedRequest);

                String response = in.readLine();
                if (response == null) break;
                System.out.println(line + ": " + response.substring(4));
            }
        } catch (IOException e) {
            System.err.println("Client error: " + e.getMessage());
        }
    }
}