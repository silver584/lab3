import java.io.*;
import java.net.*;
import java.util.Arrays;

public class Client {
    public static void main(String[] args) {
        String host = args[0];
        int port = Integer.parseInt(args[1]);
        String filePath = args[2];

        try (Socket socket = new Socket(host, port);
             PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
             BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()))) {


            try (BufferedReader fileReader = new BufferedReader(new FileReader(filePath))) {
                String line;
                while ((line = fileReader.readLine()) != null) {
                    String request = buildRequest(line);
                    if (request == null) {
                        System.err.println("Invalid line: " + line);
                        continue;
                    }
                    out.println(request);
                    String response = in.readLine();
                    System.out.println(line + ": " + response);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static String buildRequest(String line) {
        String[] parts = line.split(" ");
        if (parts.length < 2) return null;

        String op = parts[0];
        String key = parts[1];
        String value = (parts.length > 2) ? String.join(" ", Arrays.copyOfRange(parts, 2, parts.length)) : null;


        String request;
        switch (op.toUpperCase()) {
            case "PUT":
                if (value == null) return null;
                request = "P " + key + " " + value;
                break;
            case "GET":
                request = "G " + key;
                break;
            case "READ":
                request = "R " + key;
                break;
            default:
                return null;
        }


        int length = request.length() + 4;
        return String.format("%03d %s", length, request);
    }
}