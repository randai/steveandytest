package com.razor.test;


import java.lang.management.ManagementFactory;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.zeromq.ZMQ;

import com.financialogix.xstream.common.data.XStreamRates;
import com.razor.dto.RateHelper;
import com.razor.dto.RateProtos.PBMarketDataWrapper;

public class ZeromqProtoBufTest
{
	static int processNumber;
	static String connectTo;
	static String connectToStats;
	ZMQ.Context ctx;
	final AtomicLong totalMsgs = new AtomicLong(0);
	final static AtomicBoolean keepRunning = new AtomicBoolean(true);
	static int SUBJECT_COUNT = Integer.parseInt( System.getProperty("SUBJECT_COUNT","5"));
	static  CountDownLatch threadCountdown = null;
	static ExecutorService msgPool = Executors.newFixedThreadPool(SUBJECT_COUNT);
	final static BlockingQueue<MyMessage> msgQueue =  new ArrayBlockingQueue<MyMessage>(1024000);
	final static BlockingQueue<String> statsQueue =  new ArrayBlockingQueue<String>(1024000);
	class MyMessage {
		String subject;
		Long msgId;
		Long timestamp;
		byte[] msg;
	}
	static Thread queueWriterThread = null;
	static Thread mainThread = null;
	static Thread statsWriterThread = null;
	
	static int MSGS_PER_SECOND = Integer.parseInt( System.getProperty("MSGS_PER_SECOND","100"));
	
	ConcurrentHashMap<String, BlockingQueue<MyMessage>> subjectQueues = new ConcurrentHashMap<String, BlockingQueue<MyMessage>>();
	
	public static void main(String[] args)
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
		// Parse the command line arguments.
		connectTo = System.getProperty("SOCKET", "ipc:///Users/andy/ipc/rates");
		
		
		Runtime.getRuntime().addShutdownHook(new Thread() {
		    public void run() {
		    	System.out.println("Inside Add Shutdown Hook");
		        keepRunning.set(false);
		        try
				{
		        	
		        	if(queueWriterThread != null){
		        		queueWriterThread.interrupt();
		        	}
		        	if(statsWriterThread != null){
		        		statsWriterThread.interrupt();
		        	}
		        	if(mainThread != null){
		        		mainThread.interrupt();
		        	}
		        	if(msgPool != null){
		        		msgPool.shutdownNow();
		        	}
		        	threadCountdown.await();
		        	
					//System.out.println("End of Shutdown Hook : total Msgs="+totalMsgs.get());
				}
				catch (InterruptedException e)
				{
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
		        System.out.println("End of Shutdown Hook ");
		    }
		});

		new ZeromqProtoBufTest().testRun();
	}
	
	class QueueWriter implements Runnable {
		ZMQ.Socket s;
		BlockingQueue<MyMessage> queue;
		QueueWriter(BlockingQueue<MyMessage> queue){
			this.queue = queue;
			s = ctx.socket(ZMQ.PUB);
			s.bind(connectTo);

		}
		
		@Override
		public void run()
		{
			int count = 0;
			while(keepRunning.get()){
				 
				try {
					MyMessage msg = queue.take();
					Long now = System.nanoTime();
					s.send((msg.subject+"."+Long.toString(msg.msgId)).getBytes(), ZMQ.SNDMORE);
					s.send(msg.msg, 0);
					//Send info to stats server
					if(connectToStats != null)
						statsQueue.add(processNumber+","+msg.subject+","+msg.msgId+","+now.toString());
					count++;
					if(count % 1000 == 0)
						System.out.println("sending queue size = "+queue.size());
				} catch (InterruptedException e) {
					System.out.println("QueueWriter Interrupted");
					break;
				} catch(Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
					break;
				}

			}
			s.close();
			threadCountdown.countDown();
			System.out.println("Msg sending thread ended");
		}
	}
	
	class StatsWriter implements Runnable {
		ZMQ.Socket s;
		BlockingQueue<String> queue;
		Thread thisThread;
		StatsWriter(BlockingQueue<String> queue){
			this.queue = queue;
			s = ctx.socket(ZMQ.PUB);
			s.connect(connectToStats);
		}
		
		public Thread getThread(){
			return thisThread;
		}
		
		@Override
		public void run()
		{
			statsWriterThread = Thread.currentThread();
			int count = 0;
			while(keepRunning.get()){
				try {
					String msg = queue.take();
					s.send(msg.getBytes(), 0);
					count++;
					if(count % 1000 == 0)
						System.out.println("stats sending queue size = "+queue.size());
				} catch (InterruptedException e) {
					System.out.println("StatsWriter Interrupted");
					break;
				} catch(Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
					break;
				}

			}
			s.close();
			threadCountdown.countDown();
			System.out.println("Stats sending thread ended");
		}
	}
	
	void testRun()
	{
		ctx = ZMQ.context(1);
		
		// Parse the command line arguments...if no stats socket then do not collect stats
		connectToStats = System.getProperty("STATS_SOCKET");
		if(connectToStats != null){
			statsWriterThread = new Thread(new StatsWriter(statsQueue));
			statsWriterThread.start();
		}

		// Add your socket options here.
		// For example ZMQ_RATE, ZMQ_RECOVERY_IVL and ZMQ_MCAST_LOOP for PGM.

		if ( processNumber == 0 )
		{
			//To consume events to send out on socket
	        queueWriterThread = new Thread(new QueueWriter(msgQueue));
	        queueWriterThread.start();
			final int sleepInterval = 1000 / (MSGS_PER_SECOND / SUBJECT_COUNT);
			System.out.println("SERVER sleep interval = "+sleepInterval+ " ms PID = "+getJavaProcessId());
			final XStreamRates rates = GenerateXStreamRates.generate();
			threadCountdown = new CountDownLatch(SUBJECT_COUNT+1);

			for ( int portOffset = 0; portOffset < SUBJECT_COUNT; portOffset++ )
			{
				final String subjectId = Integer.toString( portOffset);
				//Start one thread per 'subject' to simulate concurrent rate processing
				msgPool.execute(new Runnable()
				{
					public void run()
					{
						
						long i = 0;
						while (keepRunning.get())
						{
							MyMessage msg = new MyMessage();
							msg.subject = subjectId;
							msg.msgId = i;
							//Serialize rate
							//long before = System.nanoTime();
							PBMarketDataWrapper pbMarketDataWrapper = RateHelper
								.convert(rates, i);
							msg.msg = pbMarketDataWrapper.toByteArray();
							long now = System.nanoTime();
							msg.timestamp = now;
							//Create a message object and put on sending queue..queue consuming thread will do sending
							msgQueue.add(msg);
							//Sleep for sleep interval
							try
							{
								Thread.sleep(sleepInterval);
							}
							catch (InterruptedException e)
							{
								System.out.println("SERVER thread for "+subjectId+" interrupted");
							}
							i++;
						}
						threadCountdown.countDown();
						System.out.println("SERVER thread "+subjectId+" finished");
					}

					public String toString()
					{
						return Thread.currentThread().getName() + ": " + subjectId;
					}
				});
			}
		} else {
			//Client consumer...start individual subject dedicated threads each reading off of their individual queues..
			for ( int portOffset = 0; portOffset < SUBJECT_COUNT; portOffset++ )
			{
				final String subjectId = Integer.toString( portOffset);
				BlockingQueue<MyMessage> subjectQueue =  new ArrayBlockingQueue<MyMessage>(100);
				subjectQueues.put(subjectId,subjectQueue);
				//Start one thread per 'subject' to simulate concurrent rate processing
				msgPool.execute(new Runnable()
				{
					public void run()
					{
						long i = 0;
						long mostRecentSeqNo = -1;
						while (keepRunning.get())
						{
							try {
								//This blocks and needs to be interrupted on exit
								MyMessage msg = (MyMessage)subjectQueues.get(subjectId).take();
								long before = System.nanoTime();
								PBMarketDataWrapper pbMarketDataWrapper = PBMarketDataWrapper.parseFrom(msg.msg);
								// See if this message on this subject is out-of-sync
								if ( pbMarketDataWrapper.getSeq() <= mostRecentSeqNo )
								{
										System.out.println("message ON SUBJECT "
												+ subjectId + " "
												+ +pbMarketDataWrapper.getSeq()
												+ " out-of-sync, latest="
												+ mostRecentSeqNo);
									continue;
								}
								mostRecentSeqNo = pbMarketDataWrapper.getSeq();
								XStreamRates rates = RateHelper
										.convert(pbMarketDataWrapper);
								
//								System.out.println(Long.toString(System
//										.currentTimeMillis())
//										+ " CLIENT ON SUBJECT " + subjectId+" RCVD "
//										+ rates.getStreamId()
//										+ " "
//										+ rates.getAllBidRates()[0].getRate()
//												.toPlainString()
//										+ "/"
//										+ rates.getAllAskRates()[0].getRate()
//												.toPlainString());
							} catch (InterruptedException ie) {
								System.out.println("CLIENT thread for "+subjectId+" interrupted");
							} catch (Exception e) {
								e.printStackTrace();
							}
						}
						threadCountdown.countDown();
						System.out.println("CLIENT thread for "+subjectId+" finished");
					}

					public String toString()
					{
						return Thread.currentThread().getName() + ": " + subjectId;
					}
				});
			}
			threadCountdown = new CountDownLatch(SUBJECT_COUNT+1);
			mainThread = Thread.currentThread();
			//On main thread read messages and dispatch to individual subject dedicated threads via their respective msgQueue
			ZMQ.Socket s = ctx.socket (ZMQ.SUB);
	        s.connect (connectTo);
	        //Subscribe to everything on this socket
	        s.subscribe(new byte[0]);
	        long mostRecentSeqNo = -1;
	        while(keepRunning.get()){
	        	try {
		        	//Read both parts
	        		byte[] firstPart = s.recv (0);
	        		if(firstPart == null) {
	        			//Hopefully this happens when interrupted
	        			break;
	        		}
		        	String fullSubject = new String(firstPart);
		            byte[] contents = s.recv (0);
		            Long now = System.nanoTime();
		        	//Extract subject and messageSeqNo
		        	String[] subjectParts = fullSubject.split("\\.");
		        	Long msgId = Long.parseLong(subjectParts[subjectParts.length-1]);
//		        	if(msgId <= mostRecentSeqNo){
//		        		System.out.println("message ON SUBJECT "
//								+ fullSubject + " is older than most recent "+mostRecentSeqNo);
//		        		continue;
//		        	}
//		        	mostRecentSeqNo = msgId;
		        	String actualSubject = fullSubject.substring(0,fullSubject.length()-(subjectParts[subjectParts.length-1].length()+1) );
		        	MyMessage msg = new MyMessage();
		        	msg.subject = actualSubject;
		        	//msg.msgId = msgId;
		        	msg.msg = contents;
		        	//This adds to the subject specific processing queue and thread..
		        	subjectQueues.get(actualSubject).add(msg);
		        	
		        	//Send to stats server
		        	statsQueue.add(processNumber+","+msg.subject+","+msgId.toString()+","+now.toString());
		        	
	        	} catch (Exception ie) {
	        		System.out.println("Exception on Client consumer");
	        		ie.printStackTrace();
	        	}
	        }
	        s.close();
	        threadCountdown.countDown();
		}


	}
	public static String getJavaProcessId () {

		return ManagementFactory.getRuntimeMXBean().getName();
		}

}
