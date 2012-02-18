package com.razor.test;

import java.math.BigDecimal;
import java.util.Date;
import java.util.Map;

import com.financialogix.streamweb.NativeWebMessageFormatHandler;
import com.financialogix.streamweb.WebMessage;
import com.financialogix.streamweb.WebMessageConstants;
import com.financialogix.streamweb.WebMessageMetaDataLookup;
import com.financialogix.xstream.common.XStreamConstants;
import com.financialogix.xstream.common.data.XStreamGlobalDate;
import com.financialogix.xstream.common.data.XStreamIdentifier;
import com.financialogix.xstream.common.data.XStreamRate;
import com.financialogix.xstream.common.data.XStreamRateHelper;
import com.financialogix.xstream.common.data.XStreamRates;
import com.financialogix.xstream.common.event.streamweb.XStreamRateMetaDataStore;

public class GenerateXStreamRates
{
	public static XStreamRates generate() {
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
			
			return rates;

		}
		catch( Throwable t )
		{
			t.printStackTrace();
			return null;
		}	
	}

}
