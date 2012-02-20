package com.razor.test;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.MulticastSocket;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.Date;
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
	public static final int PORT = 34567;
	//ff01 works with no additional routing..timetolive needs to be 0 to stay on localhost so ff(01) is not really working
	//public static final String multicastAddr = "ff01:0:0:0:0:0:2:1";
	//unix [sudo ip route add 235.0.0.1/32 dev lo] mac [sudo route -n add 235.0.0.1/32 127.0.0.1]..adds explicit multicast address to loopback...timetolive irrelevant
	public static final String multicastAddr = "235.0.0.1";
	static int PORT_COUNT = Integer.parseInt( System.getProperty("PORT_COUNT","5"));
	static ExecutorService exec = Executors.newFixedThreadPool(PORT_COUNT);
	static int MSGS_PER_SECOND = Integer.parseInt( System.getProperty("MSGS_PER_SECOND","100"));
	/**
	 * @param args
	 */
	public static void main(String[] args) throws IOException,
			InterruptedException
	{
		final AtomicBoolean keepRunning = new AtomicBoolean(true);
		final CountDownLatch threadCountdown = new CountDownLatch(PORT_COUNT);
		final AtomicLong totalMsgs = new AtomicLong(0);
		Runtime.getRuntime().addShutdownHook(new Thread() {
		    public void run() {
		    	System.out.println("Inside Add Shutdown Hook");
		        keepRunning.set(false);
		        try
				{
		        	threadCountdown.await();
					System.out.println("End of Shutdown Hook : total Msgs="+totalMsgs.get());
				}
				catch (InterruptedException e)
				{
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
		        System.exit(0);
		    }
		});
		Date date = new Date();
		
		if ( args.length > 0 && args[0].equals("server") )
		{
			final int sleepInterval = 1000 / (MSGS_PER_SECOND / PORT_COUNT);
			System.out.println("SERVER sleep interval = "+sleepInterval);
			
			final XStreamRates rates = GenerateXStreamRates.generate();

			for ( int portOffset = 0; portOffset < PORT_COUNT; portOffset++ )
			{
				final int port = portOffset + PORT;
				exec.execute(new Runnable()
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
							// p.setAddress(InetAddress.getByAddress(InetAddress.getLocalHost().getAddress()));
//							System.out.println(InetAddress.getByName(multicastAddr).getHostAddress());
//							System.out.println(Inet6Address.getByName(multicastAddr).getHostAddress());
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
								totalTime += System.nanoTime() - before;
								
//								System.out.println(Long.toString(System
//										.currentTimeMillis())
//										+ " SERVER on port "+port+" sending msg "
//										+ i
//										+ " sz "
//										+ b.length);
								i++;
								p.setData(b);
								try
								{
									ss.send(p);
								}
								catch (IOException e)
								{
									System.out.println(new Date().toString()+" "+e.getMessage());
								}
								Thread.sleep(sleepInterval);
							}
							threadCountdown.countDown();
							totalMsgs.getAndAdd(i);
							System.out.println("EXIT SERVER PUBLISHER on port "+port+" msgCount="+i+" avg="+((double)(totalTime/i)/1000000.0)+" ms");
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
		else
		{
			for ( int portOffset = 0; portOffset < PORT_COUNT; portOffset++ )
			{
				final int port = portOffset + PORT;
				exec.execute(new Runnable()
				{
					public void run()
					{
						long totalTime = 0;
						try
						{
							// String clientName = args[0];
							long mostRecentSeqNo = -1;
							System.out.println("CLIENT LISTENING ON PORT " + port);
							MulticastSocket sr = new MulticastSocket(port);
							sr.joinGroup(InetAddress.getByName(multicastAddr));
							byte[] buf = new byte[65000];
							DatagramPacket pct = new DatagramPacket(buf, buf.length);
							int i = 1;
							while (keepRunning.get())
							{
								sr.receive(pct);
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
								totalTime += System.nanoTime() - before;
								i++;
								String s = new String(buf);
								System.out.println(Long.toString(System
										.currentTimeMillis())
										+ " CLIENT ON PORT " + port+" RCVD "
										+ rates.getStreamId()
										+ " "
										+ rates.getAllBidRates()[0].getRate()
												.toPlainString()
										+ "/"
										+ rates.getAllAskRates()[0].getRate()
												.toPlainString());
							}
							threadCountdown.countDown();
							totalMsgs.getAndAdd(i);
							System.out.println("EXIT CLIENT on port "+port+" msgCount="+i+" avgUnpack="+((double)(totalTime/i)/1000000.0)+" ms");

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
}
