package com.razor.test;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.MulticastSocket;

import com.financialogix.xstream.common.data.XStreamRates;
import com.razor.dto.RateHelper;
import com.razor.dto.RateProtos.PBMarketDataWrapper;
//xxxxx
public class DatagramTest
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
			
			XStreamRates rates = null;
			
//			FileInputStream fis = new FileInputStream("/Users/andy/tmp/streamRate.obj");
//			ObjectInputStream ois = new ObjectInputStream(fis);
//			try
//			{
//				rates = (XStreamRates)ois.readObject();
//			}
//			catch (ClassNotFoundException e)
//			{
//				// TODO Auto-generated catch block
//				e.printStackTrace();
//				System.exit(0);
//			}
//			ois.close();
			
			rates = GenerateXStreamRates.generate();
			
			DatagramSocket ss = new DatagramSocket();
			ss.setBroadcast(true);
			//Dont think length matters..at send time actual will be determined..
			byte[] b = new byte[65000];
			DatagramPacket p = new DatagramPacket(b, b.length);
			p.setAddress(InetAddress.getByName(multicastAddr));
			//p.setAddress(InetAddress.getByAddress(InetAddress.getLocalHost().getAddress()));
			p.setPort(PORT);

			int i = 0;
			while (true)
			{
				//Assume its a new rate and needs converting from scratch each time
				PBMarketDataWrapper pbMarketDataWrapper = RateHelper.convert(rates,i);
				String s = new Integer(i++).toString();
				b = pbMarketDataWrapper.toByteArray();
				System.out.println(Long.toString(System.currentTimeMillis())+ " SERVER sending msg "+s+" sz "+b.length);
				p.setData(b);
				ss.send(p);
				Thread.sleep(1000);
			}
			
			
			
		}
		else
		{
			int port = PORT;
			String clientName = args[0];
			long mostRecentSeqNo = -1;
			System.out.println("CLIENT LISTENING ON PORT "+port);
			MulticastSocket sr = new MulticastSocket(port);
			sr.joinGroup(InetAddress.getByName(multicastAddr));
			byte[] buf = new byte[65000];
			DatagramPacket pct = new DatagramPacket(buf, buf.length);

			while (true)
			{
				sr.receive(pct);
				byte[] msg = new byte[pct.getLength()];
				System.arraycopy(buf, 0, msg, 0, pct.getLength());
				pct.getLength();
				//Convert byte array back to internal XStreamRates
				PBMarketDataWrapper pbMarketDataWrapper = PBMarketDataWrapper.parseFrom(msg);
				//See if this message out-of-sync
				if(pbMarketDataWrapper.getSeq() <= mostRecentSeqNo) {
					System.out.println("message "+pbMarketDataWrapper.getSeq()+" out-of-sync, latest="+mostRecentSeqNo);
					continue;
				}
				mostRecentSeqNo = pbMarketDataWrapper.getSeq();
				XStreamRates rates = RateHelper.convert(pbMarketDataWrapper);
				String s = new String(buf);
				System.out.println(Long.toString(System.currentTimeMillis())+ " CLIENT RCVD "+rates.getStreamId()+" "+rates.getAllBidRates()[0].getRate().toPlainString()+"/"+rates.getAllAskRates()[0].getRate().toPlainString());
			}
		}
	}
}
