package com.razor.test;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStreamWriter;
import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.MulticastSocket;
import java.net.Socket;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.Date;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import com.financialogix.xstream.common.data.XStreamRates;
import com.google.protobuf.InvalidProtocolBufferException;
import com.razor.dto.RateHelper;
import com.razor.dto.RateProtos.PBMarketDataWrapper;

public class ThreadingDatagramTest
{
	public static final int PORT = 34001;
	public static final int STATS_PORT = Integer.parseInt(System.getProperty("STATS_PORT","34000"));
	//ff01 works with no additional routing..timetolive needs to be 0 to stay on localhost so ff(01) is not really working
	//public static final String multicastAddr = "ff01:0:0:0:0:0:2:1";
	//unix [sudo ip route add 235.0.0.1/32 dev lo] mac [sudo route -n add 235.0.0.1/32 127.0.0.1]..adds explicit multicast address to loopback...timetolive irrelevant
	public static final String multicastAddr = "235.0.0.1";
	static int PORT_COUNT = Integer.parseInt( System.getProperty("PORT_COUNT","5"));
	static ExecutorService msgPool = Executors.newFixedThreadPool(PORT_COUNT);
	static ExecutorService statsPool = Executors.newFixedThreadPool(PORT_COUNT);
	static int MSGS_PER_SECOND = Integer.parseInt( System.getProperty("MSGS_PER_SECOND","100"));
	
	static Socket socket = null;
	
	final static BlockingQueue<String> queue =  new ArrayBlockingQueue<String>(1024000);
	
	
	
	static String commandLine[];
	static int processNumber;
	
	Thread statsWriter;
	
	final AtomicBoolean keepRunning = new AtomicBoolean(true);
	final CountDownLatch threadCountdown = new CountDownLatch(PORT_COUNT);

	
	/**
	 * @param args
	 */
	
	public static void main(String[] args) throws IOException,
			InterruptedException
	{
		if(args.length != 1){
			System.out.println("Usage : [0-n] where 0 means server and n is client id starting at 1");
			System.exit(1);
		}
		try
		{
			processNumber = Integer.parseInt(args[0]);
		}
		catch (NumberFormatException e)
		{
			System.out.println("Usage : [0-n] where 0 means server and n is client id starting at 1");
			System.exit(1);
		}
		
		commandLine = args;
		new ThreadingDatagramTest().testRun();
	}
	
	void testRun() {
		// See it stats server is running..
		try {
			//
			// Create a connection to the server socket on the server
			// application
			//
			socket = new Socket(InetAddress.getLocalHost().getHostName(), STATS_PORT);
//	        final ObjectOutputStream oos = (socket != null ? new ObjectOutputStream(socket.getOutputStream()) : null);
			BufferedWriter wr = (socket != null ? new BufferedWriter(new OutputStreamWriter(socket.getOutputStream())): null);
	        
        	//To consume events to send to stats server
	        statsWriter = new Thread(new StatsWriter(wr, queue));
	        statsWriter.start();

		} catch (UnknownHostException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}		
		final boolean statsMode = socket != null ? true : false;
		if(statsMode){
			System.out.println("Connected to stats server");
		}else{
			System.out.println("NOT CONNECTED to stats server");
		}
		
		
		
		final AtomicLong totalMsgs = new AtomicLong(0);
		Runtime.getRuntime().addShutdownHook(new Thread() {
		    public void run() {
		    	System.out.println("Inside Add Shutdown Hook");
		        keepRunning.set(false);
		        try
				{
		        	threadCountdown.await();
		        	
		        	if(statsWriter != null){
		        		statsWriter.interrupt();
		        	}
		        	
					System.out.println("End of Shutdown Hook : total Msgs="+totalMsgs.get());
				}
				catch (InterruptedException e)
				{
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
		    }
		});
        // Send a message to the stats application
        //
		if(statsMode)
        	queue.add(Integer.toString(processNumber));
		//If im the server indicated by 0...
		if ( processNumber == 0 )
		{
			final int sleepInterval = 1000 / (MSGS_PER_SECOND / PORT_COUNT);
			System.out.println("SERVER sleep interval = "+sleepInterval+ " ms PID = "+getJavaProcessId());
			final XStreamRates rates = GenerateXStreamRates.generate();

			for ( int portOffset = 0; portOffset < PORT_COUNT; portOffset++ )
			{
				final int port = portOffset + PORT;
				msgPool.execute(new Runnable()
				{
					public void run()
					{
						long totalTime = 0;
						try
						{
							System.out.println("SERVER PUBLISHER on port "+port);
							MulticastSocket ss = new MulticastSocket();
							//ss.setBroadcast(true);
							ss.joinGroup(InetAddress.getByName(multicastAddr));
							//Stay on localhost ..irrelevant if via route add
							ss.setTimeToLive(0);
							// Dont think length matters..at send time actual will
							// be determined..
							byte[] b = new byte[65000];
							DatagramPacket p = new DatagramPacket(b, b.length);
							p.setAddress(InetAddress.getByName(multicastAddr));
							p.setPort(port);

							int i = 0;
							while (keepRunning.get())
							{
								// Assume its a new rate and needs converting from
								// scratch each time
								long before = System.nanoTime();
								PBMarketDataWrapper pbMarketDataWrapper = RateHelper
										.convert(rates, i);
								b = pbMarketDataWrapper.toByteArray();
								long now = System.nanoTime();
								//Only start collecting stats on/after 10th time
								if(i>9)
									totalTime += now - before;
								
//								System.out.println(Long.toString(System
//										.currentTimeMillis())
//										+ " SERVER on port "+port+" sending msg "
//										+ i
//										+ " sz "
//										+ b.length);
								p.setData(b);
								try
								{
									//Time stamp before send
									ss.send(p);
									//Send timestamp and message id to stats server..0 means server
									if(statsMode)
										queue.add(processNumber+","+port+","+i+","+now);
									if(i % 1000 == 0 && port == PORT)
										System.out.println(new Date().toString()+ " msgCount="+i+" avgMsgPack="+((double)(totalTime/(i-10))/1000000.0)+" ms");

								}
								catch (IOException e)
								{
									System.out.println(new Date().toString()+" "+e.getMessage());
								}
								i++;

								Thread.sleep(sleepInterval);
							}
							if(statsMode)
								queue.add(processNumber+",bye");
							threadCountdown.countDown();
							totalMsgs.getAndAdd(i);
							System.out.println("EXIT SERVER PUBLISHER on port "+port+" msgCount="+i+" avgMsgPack="+((double)(totalTime/(i-10))/1000000.0)+" ms");
						}
						catch (SocketException e)
						{
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
						catch (UnknownHostException e)
						{
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
						catch (IOException e)
						{
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
						catch (InterruptedException e)
						{
							// TODO Auto-generated catch block
							e.printStackTrace();
						}

					}

					public String toString()
					{
						return Thread.currentThread().getName() + ": " + port;
					}
				});
			}
		}
		//Else if i'm the consumer i.e. a client
		else
		{
			for ( int portOffset = 0; portOffset < PORT_COUNT; portOffset++ )
			{
				final int port = portOffset + PORT;
				msgPool.execute(new Runnable()
				{
					public void run()
					{
						long totalTime = 0;
						try
						{
							// String clientName = args[0];
							long mostRecentSeqNo = -1;
							System.out.println("CLIENT LISTENING ON PORT " + port+" PID = "+getJavaProcessId());
							MulticastSocket sr = new MulticastSocket(port);
							sr.joinGroup(InetAddress.getByName(multicastAddr));
							byte[] buf = new byte[2000];
							DatagramPacket pct = new DatagramPacket(buf, buf.length);
							int i = 1;
							while (keepRunning.get())
							{
								sr.receive(pct);
								long recvd = System.nanoTime();
								byte[] msg = new byte[pct.getLength()];
								System.arraycopy(buf, 0, msg, 0, pct.getLength());
								// Convert byte array back to internal XStreamRates
								
								long before = System.nanoTime();
								PBMarketDataWrapper pbMarketDataWrapper = PBMarketDataWrapper
										.parseFrom(msg);
								// See if this message out-of-sync
								if ( pbMarketDataWrapper.getSeq() <= mostRecentSeqNo )
								{
									System.out.println("message ON PORT " + port+" "+
											+ pbMarketDataWrapper.getSeq()
											+ " out-of-sync, latest="
											+ mostRecentSeqNo);
									continue;
								}
								mostRecentSeqNo = pbMarketDataWrapper.getSeq();
								XStreamRates rates = RateHelper
										.convert(pbMarketDataWrapper);
								//Only start collecting stats on/after 100th time
								if(i>100)
									totalTime += System.nanoTime() - before;
								i++;
								
								//Send timestamp and message id to stats
								if(statsMode)
									queue.add(processNumber+","+port+","+mostRecentSeqNo+","+recvd);
								
//								System.out.println(Long.toString(System
//										.currentTimeMillis())
//										+ " CLIENT ON PORT " + port+" RCVD "
//										+ rates.getStreamId()
//										+ " "
//										+ rates.getAllBidRates()[0].getRate()
//												.toPlainString()
//										+ "/"
//										+ rates.getAllAskRates()[0].getRate()
//												.toPlainString());
								if(i % 1000 == 0 && port == PORT)
									System.out.println(new Date().toString()+ " msgCount="+i+" avgMsgUnpack="+((double)(totalTime/(i-100))/1000000.0)+" ms");

							}
							if(statsMode)
								queue.add(processNumber+",bye");
							threadCountdown.countDown();
							totalMsgs.getAndAdd(i);
							System.out.println("EXIT CLIENT on port "+port+" msgCount="+i+" avgMsgUnpack="+((double)(totalTime/(i-100))/1000000.0)+" ms");

						}
						catch (UnknownHostException e)
						{
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
						catch (InvalidProtocolBufferException e)
						{
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
						catch (IOException e)
						{
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}

					public String toString()
					{
						return Thread.currentThread().getName() + ": " + port;
					}
				});
			}
		}
	}
	public static String getJavaProcessId () {

		return ManagementFactory.getRuntimeMXBean().getName();
//			try {
//				RuntimeMXBean runtimeMXBean = ManagementFactory.getRuntimeMXBean();
//				Field jvmField = runtimeMXBean.getClass().getDeclaredField("jvm");
//				jvmField.setAccessible(true);
//				vmManagement vmManagement = (VMManagement) jvmField.get(runtimeMXBean);
//				Method getProcessIdMethod = vmManagement.getClass().getDeclaredMethod("getProcessId");
//				getProcessIdMethod.setAccessible(true);
//				Integer processId = (Integer) getProcessIdMethod.invoke(vmManagement);
//				System.out.println("################    ProcessId = " + processId);
//				return processId;
//			} catch (Exception e) {
//				e.printStackTrace();
//			}
		}
	
	class StatsWriter implements Runnable {
		BufferedWriter wr;
		BlockingQueue<String> queue;
		StatsWriter(BufferedWriter wr, BlockingQueue<String> queue){
			this.wr = wr;
			this.queue = queue;
		}
		
		@Override
		public void run()
		{
			int count = 0;
			while(true){
				 
				try {
					String msg = queue.take();
					if(msg.contains("bye")) {
						if(wr != null){
							wr.write(msg+"\n");
							wr.flush();
						}
						break;
					}
					if(wr != null){
						wr.write(msg+"\n");
						wr.flush();
					}
					if(keepRunning.get() == false){
						if(queue.size() == 0){
							break;
						}
					}
					
					count++;
					if(count % 1000 == 0)
						System.out.println("stats queue size = "+queue.size());
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
					break;
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
					break;
				}

			}
			System.out.println("Stats sending thread ended");
		}
		
	}
	


}
