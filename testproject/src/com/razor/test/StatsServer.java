package com.razor.test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.lang.ClassNotFoundException;
import java.lang.Runnable;
import java.lang.Thread;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

public class StatsServer {
    private ServerSocket server;
    private int port = Integer.parseInt(System.getProperty("STATS_PORT","34000"));

   static Thread[] connectedTheads;
   
   static long allConnectedAt = 0;
    
   static ConcurrentHashMap<String, Long[]> msgTimes = new ConcurrentHashMap<String, Long[]>();
   
    public StatsServer() {
        try {
            server = new ServerSocket(port);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    boolean allConnected() {
    	for(int i=0;i<connectedTheads.length;i++) {
    		if(connectedTheads[i] == null)
    			return false;
    	}
    	if(allConnectedAt == 0) {
    		System.out.println("All connected at "+new Date().toString()+", will start collecting stats in 10 seconds");
    		allConnectedAt = System.nanoTime()+(10000000000L);
    	}
    	return true;
    }
    int noOfOutlyers = 0;
    boolean allTimesPresent(Long[] times,String key){
    	for(int i=0;i<times.length;i++) {
    		if(times[i] == null)
    			return false;
    	}
    	boolean testForOutlyers = false;
       	for(int i=1;i<times.length;i++) {
    		if((times[i] - times[0])/1000000.0 > 4 ){
    			System.out.println(new Date().toString()+ " Outlyer on "+key+" from "+i+" took "+(times[i] - times[0])/1000000.0+" ms");
    			noOfOutlyers++;
    			testForOutlyers = true;
    			long tripTime = times[i] - times[0];
    			if(tripTime > maxOverallTripTime)
        			maxOverallTripTime = tripTime;
    		}
    	}
       	if(testForOutlyers == true)
       		return false;
       	
    	return true;
     }
    
    public static void main(String[] args) {
    	if(args.length != 1){
			System.out.println("Usage : [n] where n is number of clients that will be run.");
			System.exit(1);
		}
		try
		{
			connectedTheads = new Thread[Integer.parseInt(args[0])+1];
		}
		catch (NumberFormatException e)
		{
			System.out.println("Usage : [0-n] where 0 means server and n is client id starting at 1");
			System.exit(1);
		}
    	StatsServer example = new StatsServer();
        example.handleConnection();
    }

    public void handleConnection() {
        System.out.println("Waiting for client connection");

        while (true) {
            try {
                Socket socket = server.accept();
                new ConnectionHandler(socket, connectedTheads);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
    
    class ConnectionHandler implements Runnable {
        private Socket socket;
        Thread t;
        Thread[] connectedThreads;
        public ConnectionHandler(Socket socket, Thread[] connectedThreads) {
            this.socket = socket;
            t = new Thread(this);
            t.start();
            this.connectedThreads = connectedThreads;
        }

        public void run() {
            try
            {
                //
                // Read inital message sent by client application
                //
            	BufferedReader reader = new BufferedReader(new InputStreamReader(
                        socket.getInputStream()));
                //ObjectInputStream ois = new ObjectInputStream(socket.getInputStream());
               // String message = (String) ois.readObject();
            	String message = reader.readLine();
                Integer idx = null;
                try
    			{
                	idx = new Integer(message);
    			}
    			catch (NumberFormatException e)
    			{
    				System.out.println("Invalid initial Message Received : " + message+" ...ignoring");
    				return;
    			}
                System.out.println("Initial Message Received from "+idx);
                //Set the appropriate entry for this connection
                connectedThreads[idx.intValue()] = t;
            	//System.out.println("Message Received: " + message);
                while(idx != null) {
                	try
					{
//						message = (String) ois.readObject();
                		message = reader.readLine();
					}
					catch (Exception e)
					{
						e.printStackTrace();
						continue;
					}
                	if(message.contains("bye"))
                		break;
                	//Don't start collecting stats unless all are connected..
                	if(allConnected() == false)
                		continue;
                	String[] msgParts = message.split(",");
                	if(msgParts.length == 4){
                		//Check that the time in the message is at least 10 seconds in the future from the time allConnected detected
                		long msgTime =  Long.parseLong(msgParts[3]);
                		if(msgTime <= allConnectedAt)
                			continue;
                		//extract the socket/msg to make a key
                		String key = msgParts[1]+","+msgParts[2];
                		Long[] newTimes = new Long[connectedThreads.length]; 
                		//Create an entry in hashmap
                		Long[] times = msgTimes.putIfAbsent(key,newTimes);
                		if(times == null)
                			times = newTimes;
                		times[Integer.parseInt(msgParts[0])] = Long.parseLong(msgParts[3]);
                		if(allTimesPresent(times,key)){
                			calculateAverageTime(key,times);
                		}
                	} else {
                		System.out.println("Garbage Message Received: " + message);
                	}
                }
                //Remove ourselves from connected table
                connectedThreads[idx.intValue()] = null;
                //ois.close();
                reader.close();
                socket.close();
                System.out.println("process "+idx+" disconnected");
                if(idx == 0){
                	//Its the server so dump out the statistics
                	printStats();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
    
    double totalOverallTripTime = 0;
    long totalOverallMsgCount = 0;
    double maxOverallTripTime = 0;
    double minOverallTripTime = Double.MAX_VALUE;
    
    double totalAvgTripTime = 0;
    long totalAvgMsgCount = 0;
    double maxAvgTripTime = 0;
    double minAvgTripTime = Double.MAX_VALUE;
    
    void calculateAverageTime(String key, Long[] times) {
    	long totalTripTime = 0;
    	for(int i=1;i<times.length;i++){
    		long tripTime = (times[i].longValue() - times[0].longValue());
    		totalTripTime += tripTime;
    		
    		totalOverallTripTime += tripTime;
    		if(tripTime > maxOverallTripTime)
    			maxOverallTripTime = tripTime;
    		if(tripTime < minOverallTripTime)
    			minOverallTripTime = tripTime;
    		totalOverallMsgCount++;
    	}
    	double avgTripTime = totalTripTime / (times.length - 1);
    	
    	totalAvgTripTime += avgTripTime;
    	if(avgTripTime > maxAvgTripTime)
    		maxAvgTripTime = avgTripTime;
    	if(avgTripTime < minAvgTripTime)
    		minAvgTripTime = avgTripTime;
    	totalAvgMsgCount +=  1;
    	
    	msgTimes.remove(key);
    	
    	if(totalAvgMsgCount % 1000 == 0){
    		printAvg();
    	}
    }
    void printAvg() {
    	System.out.println(new Date().toString()+" Overall Avg trip time = "+(totalOverallTripTime/totalOverallMsgCount)/1000000.0+"ms for "+totalOverallMsgCount+" msgs, max="+maxOverallTripTime/1000000.0+" min="+minOverallTripTime/1000000.0+", totalOutlyers="+noOfOutlyers);
//    	System.out.println("Topic Avg trip time = "+(totalAvgTripTime/totalAvgMsgCount)/1000000.0+"ms for "+totalAvgMsgCount+" msgs,max="+maxAvgTripTime/1000000.0+" min="+minAvgTripTime/1000000.0);
   	
    }
    void printStats() {
    	printAvg();
       	System.out.println("Client Count = "+(connectedTheads.length-1));
       	
       	List<String> list = new ArrayList<String>(msgTimes.keySet());
       	for(int j=0;j<list.size();j++){
       		String key = list.get(j);
       		System.out.print("\nMsg "+key+" ");
       		Long[] times = msgTimes.get(key);
       		for(int i=0;i<times.length;i++){
       			System.out.print(" "+Integer.toString(i)+"="+(times[i]==null?"null" : times[i]));
       		}
       		
       	}
       	
    }
}



