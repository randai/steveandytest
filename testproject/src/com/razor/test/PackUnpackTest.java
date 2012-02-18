package com.razor.test;

import com.financialogix.xstream.common.data.XStreamRates;
import com.google.protobuf.InvalidProtocolBufferException;
import com.razor.dto.RateHelper;
import com.razor.dto.RateProtos.PBMarketDataWrapper;

public class PackUnpackTest
{
	final static  int count = 1000000;
	/**
	 * @param args
	 */
	public static void main(String[] args)
	{
		
		XStreamRates rates = GenerateXStreamRates.generate();
		byte[] b = null;
		long totalTime = 0;
		for(int i=0;i<count;i++){
			long before = System.nanoTime();
			PBMarketDataWrapper pbMarketDataWrapper = RateHelper
					.convert(rates, 1);
			b = pbMarketDataWrapper.toByteArray();
			totalTime += System.nanoTime() - before;
		}
		System.out.println("Pack = "+((double)totalTime/(double)count) / 1000000.0 + " ms");
		
		for(int i=0;i<count;i++){
			long before = System.nanoTime();
			try
			{
				PBMarketDataWrapper pbMarketDataWrapper = PBMarketDataWrapper.parseFrom(b);
				rates = RateHelper.convert(pbMarketDataWrapper);
				totalTime += System.nanoTime() - before;
			}
			catch (InvalidProtocolBufferException e)
			{
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		System.out.println("Unpack = "+((double)totalTime/(double)count) / 1000000.0 + " ms");
		
		
	}

}
