package com.razor.test;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.lang.ClassNotFoundException;
import java.lang.Runnable;
import java.lang.Thread;
import java.net.ServerSocket;
import java.net.Socket;

public class StatsServer {
    private ServerSocket server;
    private int port = Integer.parseInt(System.getProperty("STATS_PORT","34000"));

    public StatsServer() {
        try {
            server = new ServerSocket(port);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
    	StatsServer example = new StatsServer();
        example.handleConnection();
    }

    public void handleConnection() {
        System.out.println("Waiting for client connection");

        while (true) {
            try {
                Socket socket = server.accept();
                new ConnectionHandler(socket);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}

class ConnectionHandler implements Runnable {
    private Socket socket;

    public ConnectionHandler(Socket socket) {
        this.socket = socket;

        Thread t = new Thread(this);
        t.start();
    }

    public void run() {
        try
        {
            //
            // Read a message sent by client application
            //
            ObjectInputStream ois = new ObjectInputStream(socket.getInputStream());
            String message = (String) ois.readObject();
        	System.out.println("Message Received: " + message);
            while(true) {
            	message = (String) ois.readObject();
            	System.out.println("Message Received: " + message);
            	if(message != null && message.equals("bye"))
            		break;

            }
            ois.close();
            socket.close();
            System.out.println("Client disconnected");
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    }
}

