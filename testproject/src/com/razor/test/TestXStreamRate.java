package com.razor.test;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeSet;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.ObjectBuffer;
import com.esotericsoftware.kryo.serialize.BigDecimalSerializer;
import com.financialogix.streamweb.NativeWebMessageFormatHandler;
import com.financialogix.streamweb.WebMessageConstants;
import com.financialogix.streamweb.WebMessageMetaDataLookup;
import com.financialogix.streamweb.WebMessage;
import com.financialogix.xstream.common.XStreamConstants;
import com.financialogix.xstream.common.data.XStreamGlobalDate;
import com.financialogix.xstream.common.data.XStreamIdentifier;
import com.financialogix.xstream.common.data.XStreamRate;
import com.financialogix.xstream.common.data.XStreamRateFilter;
import com.financialogix.xstream.common.data.XStreamRateHelper;
import com.financialogix.xstream.common.data.XStreamRates;
import com.financialogix.xstream.common.event.streamweb.XStreamRateMetaDataStore;
import com.razor.dto.RateHelper;
import com.razor.dto.RateProtos.PBMarketDataWrapper;


public class TestXStreamRate
{
	public static void	main
						( String[]	args )
	{
		String formattedRate = null;
		byte[] rateBytes = null;
		long totalFormatTime = 0;
		long totalParseTime = 0;
		long totalConvertToTime = 0;
		long totalConvertFromTime = 0;
		int noOfSamples = 100000;
		//int noOfSamples = 100;
		try
		{
			XStreamRateMetaDataStore metaDataStore = new XStreamRateMetaDataStore();
			WebMessageMetaDataLookup lookup = new WebMessageMetaDataLookup();
			lookup.registerTopicPrefixMetaData( XStreamConstants.EVENT_TOPIC_RATE_UPDATE,
					metaDataStore.getTopicPrefixMetaData().get( XStreamConstants.EVENT_TOPIC_RATE_UPDATE ) );
			NativeWebMessageFormatHandler webMessageFormatHandler = new NativeWebMessageFormatHandler( lookup );
			
			XStreamRate[] bidRates = new XStreamRate[5];
			XStreamRate[] askRates = new XStreamRate[5];
			XStreamIdentifier identifier = new XStreamIdentifier( "USD.CAD.SPOT" );
			for( int count=0; count<5; count++ )
			{
				bidRates[count] = new XStreamRate( identifier, 
													new XStreamGlobalDate(),
													String.valueOf( System.currentTimeMillis() ),
													true,
													true,
													new BigDecimal( "1000000.00" ),
													new BigDecimal( "1.234564" ),
													new BigDecimal( "1.23456" ),
													new BigDecimal( "0.000045" ),
													new Date() );
				askRates[count] = new XStreamRate( identifier, 
													new XStreamGlobalDate(),
													String.valueOf( System.currentTimeMillis() ),
													false,
													true,
													new BigDecimal( "1000000.00" ),
													new BigDecimal( "1.234564" ),
													new BigDecimal( "1.23456" ),
													new BigDecimal( "0.000045" ),
													new Date() );
			}
			XStreamRates rates = new XStreamRates( String.valueOf( System.currentTimeMillis() ),
													identifier,
													new XStreamGlobalDate(),
													bidRates,
													askRates,
													new Date() );

			for( int count=0; count<noOfSamples; count++ )
			{
				long currentTime = System.nanoTime();
				Map<String,String> values = XStreamRateHelper.Convert( rates );
				totalConvertToTime += System.nanoTime() - currentTime;
				WebMessage webMessage = new WebMessage( XStreamConstants.EVENT_TOPIC_RATE_UPDATE, 
														System.currentTimeMillis(),
														System.currentTimeMillis() );
				webMessage.addFields( values );
				currentTime = System.nanoTime();
				formattedRate = webMessageFormatHandler.formatMessage( webMessage, false );
				totalFormatTime += System.nanoTime() - currentTime;
				// add sequence no
				formattedRate = "12345" + WebMessageConstants.NATIVE_FORMAT_MESSAGE_SEPARATOR + formattedRate;
				currentTime = System.nanoTime();
				webMessage = webMessageFormatHandler.parseMessage( formattedRate, System.currentTimeMillis() );
				totalParseTime += System.nanoTime() - currentTime;
				currentTime = System.nanoTime();
				XStreamRates newRates = XStreamRateHelper.Convert( webMessage.getAllFields() );
				totalConvertFromTime += System.nanoTime() - currentTime;
			}
			
			System.out.println( "StreamWeb Performance" );
			System.out.println( "=====================" );
			System.out.println( "No of bytes " + formattedRate.getBytes().length );
			System.out.println( "Avg Convert To Time " + totalConvertToTime/(long)noOfSamples );
			System.out.println( "Avg Format Time " + totalFormatTime/(long)noOfSamples );
			System.out.println( "Avg Parse Time " + totalParseTime/(long)noOfSamples );
			System.out.println( "Avg Convert From Time " + totalConvertFromTime/(long)noOfSamples );

			
			Kryo kryo = new Kryo();
			kryo.register( ArrayList.class );
			kryo.register( BigDecimal.class, new BigDecimalSerializer() );
			kryo.register( Date.class );
			kryo.register( TreeSet.class );
			kryo.register( HashMap.class );
			kryo.register( XStreamRates.class );
			kryo.register( XStreamRate.class );
			kryo.register( XStreamIdentifier.class, new XStreamIdentifierSerializer() );
			kryo.register( XStreamGlobalDate.class );
			kryo.register( XStreamRateFilter.class );
			
			totalConvertToTime = 0;
			totalFormatTime = 0;
			totalParseTime = 0;
			totalConvertFromTime = 0;
			
			for( int count=0; count<noOfSamples; count++ )
			{
				long currentTime = System.nanoTime();
				ObjectBuffer buffer = new ObjectBuffer( kryo );
				rateBytes = buffer.writeObject( rates );
				totalConvertToTime += System.nanoTime() - currentTime;
				currentTime = System.nanoTime();
				XStreamRates newRates = (XStreamRates)buffer.readObject( rateBytes, XStreamRates.class );
				totalConvertFromTime += System.nanoTime() - currentTime;
			}		
			
			System.out.println( "\n\nKryo Performance" );
			System.out.println( "=====================" );
			System.out.println( "No of bytes " + rateBytes.length );
			System.out.println( "Avg Convert To Time " + totalConvertToTime/(long)noOfSamples );
			System.out.println( "Avg Convert From Time " + totalConvertFromTime/(long)noOfSamples );
			
			totalConvertToTime = 0;
			totalFormatTime = 0;
			totalParseTime = 0;
			totalConvertFromTime = 0;
			
			for( int count=0; count<noOfSamples; count++ )
			{
				long currentTime = System.nanoTime();
				PBMarketDataWrapper pbMarketDataWrapper = RateHelper
						.convert(rates, 1);
				rateBytes = pbMarketDataWrapper.toByteArray();
				totalConvertToTime += System.nanoTime() - currentTime;
				currentTime = System.nanoTime();
				pbMarketDataWrapper = PBMarketDataWrapper.parseFrom(rateBytes);
				rates = RateHelper.convert(pbMarketDataWrapper);
				totalConvertFromTime += System.nanoTime() - currentTime;
			}		
			
			System.out.println( "\n\nProtoBuf Performance" );
			System.out.println( "=====================" );
			System.out.println( "No of bytes " + rateBytes.length );
			System.out.println( "Avg Convert To Time " + totalConvertToTime/(long)noOfSamples );
			System.out.println( "Avg Convert From Time " + totalConvertFromTime/(long)noOfSamples );
			
			
		}
		catch( Throwable t )
		{
			t.printStackTrace();
		}	
	}
}
