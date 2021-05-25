package com.qunar.kafkamonitor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.SocketFactory;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.regex.Pattern;

public class GraphiteUtil {

    private static final Pattern POINT = Pattern.compile("\\.");

    private String hostName;
    private int port;
    private SocketFactory socketFactory;

    private Socket socket;
    private PrintWriter writer;

    private static final Logger LOGGER = LoggerFactory.getLogger(GraphiteUtil.class);

    public GraphiteUtil(String hostName, int port) {
        this(hostName, port, SocketFactory.getDefault());
    }

    public GraphiteUtil(String hostName, int port, SocketFactory socketFactory) {
        this.hostName = hostName;
        this.port = port;
        this.socketFactory = socketFactory;
    }

    public void connect() throws IOException {
        if (isConnected()) {
            throw new IllegalStateException("Already connected");
        }
        this.socket = socketFactory.createSocket(hostName, port);
        this.writer = new PrintWriter(socket.getOutputStream());
    }

    public boolean isConnected() {
        return socket != null && socket.isConnected() && !socket.isClosed();
    }

    public void send(String content) throws IOException {

        writer.println(content);
    }

    public void flush() {
        if (writer != null) {
            writer.flush();
        }
    }

    public void close() {

        if (writer != null) {
            writer.close();
            this.writer = null;
        }

        try {
            if (socket != null) {
                socket.close();
            }
        } catch (IOException ex) {
            LOGGER.debug("Error closing socket", ex);
        } finally {
            this.socket = null;
        }
    }

    protected String sanitize(String s) {
        return POINT.matcher(s).replaceAll("_");
    }


}