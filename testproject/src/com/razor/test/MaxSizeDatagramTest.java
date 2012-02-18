package com.razor.test;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.MulticastSocket;

public class MaxSizeDatagramTest
{
	public static final int PORT = 34567;
	public static final String multicastAddr = "235.1.1.1";
	/**
	 * @param args
	 */
	public static void main(String[] args) throws IOException,
			InterruptedException
	{
		if ( args.length > 0 && args[0].equals("server") )
		{
			System.out.println("SERVER PUBLISHER");
			DatagramSocket ss = new DatagramSocket();
			ss.setBroadcast(true);
			
			StringBuffer sb = new StringBuffer(65000);
			for(int x=0;x<65000;x++){
				sb.append("X");
			}
			sb.replace(64999, 65000, "Y");
			byte[] b = sb.toString().getBytes();
			DatagramPacket p = new DatagramPacket(b, b.length);
			p.setAddress(InetAddress.getByName(multicastAddr));
			//p.setAddress(InetAddress.getByAddress(InetAddress.getLocalHost().getAddress()));
			p.setPort(PORT);

			int i = 0;
			while (true)
			{
				String s = new Integer(i++).toString();
				System.out.println(Long.toString(System.currentTimeMillis())+ " SERVER sending :"+b.length+" bytes");
				//b = s.getBytes();
				p.setData(b);
				try {
				ss.send(p);
				} catch (Exception e) {
					e.printStackTrace();
				}
				Thread.sleep(1000);
			}
			
			
			
		}
		else
		{
			int port = PORT;
			String clientName = args[0];

			System.out.println("CLIENT LISTENING ON PORT "+port);
			MulticastSocket sr = new MulticastSocket(port);
			sr.joinGroup(InetAddress.getByName(multicastAddr));
			while (true)
			{
				byte[] buf = new byte[65000];
				DatagramPacket pct = new DatagramPacket(buf, buf.length);
				sr.receive(pct);
				String sb = new String(buf).substring(0,pct.getLength());
				String end = sb.substring(64999,65000);
				System.out.println(Long.toString(System.currentTimeMillis())+ " CLIENT "+clientName+" RECV "+sb.length()+" bytes, endbyte=" +end+ " "
						+ pct.getAddress().toString());
			}
		}
	}
}
