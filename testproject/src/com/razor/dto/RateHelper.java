package com.razor.dto;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import com.financialogix.common.util.StringHelper;
import com.financialogix.xstream.common.data.XStreamGlobalDate;
import com.financialogix.xstream.common.data.XStreamIdentifier;
import com.financialogix.xstream.common.data.XStreamRate;
import com.financialogix.xstream.common.data.XStreamRates;
import com.razor.dto.RateProtos.PBMarketData;
import com.razor.dto.RateProtos.PBMarketDataWrapper;
import com.razor.dto.RateProtos.PBMarketRate;


public class RateHelper
{
	/**
	 * Convert from protocol buffer version to internal version
	 * @param pb
	 * @return
	 */
	public static XStreamRates convert(PBMarketDataWrapper pb){
		
		try {
			XStreamIdentifier identifier = new XStreamIdentifier(pb.getMarketData().getProductId1(),pb.getMarketData().getProductId2(),pb.getMarketData().getTerm());
			
			List<XStreamRate> bidRates = new ArrayList<XStreamRate>();
			for(PBMarketRate pbrBid : pb.getMarketData().getBidRatesList() ) {
				bidRates.add( new XStreamRate(pb.getMarketData().getProviderID(),pbrBid.getSourceId(),identifier,XStreamGlobalDate.ParseDate(pb.getMarketData().getSettlementDate()),pbrBid.getRateId(),true,pbrBid.getIsExecutable(),new BigDecimal(pbrBid.getLimit()),new BigDecimal(pbrBid.getRate()),pbrBid.getIsCancelled(),pbrBid.getCancellationReason(),new Date(Long.parseLong(pbrBid.getTimestamp()))));
			}
			List<XStreamRate> askRates = new ArrayList<XStreamRate>();
			for(PBMarketRate pbrAsk : pb.getMarketData().getAskRatesList() ) {
				askRates.add( new XStreamRate(pb.getMarketData().getProviderID(),pbrAsk.getSourceId(),identifier,XStreamGlobalDate.ParseDate(pb.getMarketData().getSettlementDate()),pbrAsk.getRateId(),true,pbrAsk.getIsExecutable(),new BigDecimal(pbrAsk.getLimit()),new BigDecimal(pbrAsk.getRate()),pbrAsk.getIsCancelled(),pbrAsk.getCancellationReason(),new Date(Long.parseLong(pbrAsk.getTimestamp()))));
			}
			XStreamRate[] bidRatesArray = bidRates.toArray(new XStreamRate[0]);
			XStreamRate[] askRatesArray = askRates.toArray(new XStreamRate[0]);
			XStreamRates xrs = new XStreamRates(pb.getMarketData().getRatesId(),identifier,XStreamGlobalDate.ParseDate(pb.getMarketData().getSettlementDate()),bidRatesArray,askRatesArray,new Date(pb.getMarketData().getTimestamp()));
			return xrs;
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
	}
	
	/**
	 * Convert from internal version to protocol buffer version
	 * @param xrs
	 * @param seqNo
	 * @return
	 */
	public static PBMarketDataWrapper convert(XStreamRates xrs,long seqNo){
		PBMarketDataWrapper.Builder bmdw = PBMarketDataWrapper.newBuilder();
		bmdw.setSeq(seqNo);
		PBMarketData.Builder bmd = PBMarketData.newBuilder();
		String providerID;
		
		if ( xrs.getFirstBidRateProviderId() != null )
		{
			providerID = xrs.getFirstBidRateProviderId();
		}
		else
		{
			providerID = xrs.getFirstAskRateProviderId();
		}
		if(providerID != null)
			bmd.setProviderID(providerID);
		if(xrs.getRatesId() != null)
			bmd.setRatesId(xrs.getRatesId());
		bmd.setProductId1(xrs.getProduct1());
		bmd.setProductId2(xrs.getProduct2());
		bmd.setTerm(xrs.getTerm());
		bmd.setTimestamp(xrs.getTimestamp().getTime());
		if(xrs.getSettlementDate() != null) {
			bmd.setSettlementDate(StringHelper.ConvertDate(xrs.getSettlementDate()));
		}
		bmd.setLastDealt(xrs.isLastDealt());
		
		//Builder for market rate
		PBMarketRate.Builder bmr = PBMarketRate.newBuilder();
	
		for(XStreamRate rate : xrs.getAllBidRates()){
			bmr.clear();
			bmr.setRateId(rate.getRateId());
			if(rate.getSourceId() != null)
				bmr.setSourceId(rate.getSourceId());
			if(rate.getLimit() != null)
				bmr.setLimit(  rate.getLimit().toPlainString());
			if(rate.getMinimumLimit() != null )
				bmr.setMinLimit(rate.getMinimumLimit().toPlainString());
			if(rate.getRate() != null)
				bmr.setRate( rate.getRate().toPlainString());
			if(rate.getSpotRate() != null)
				bmr.setSpotRate(rate.getSpotRate().toPlainString());
			if(rate.getForwardRate() != null)
				bmr.setForwardRate(rate.getForwardRate().toPlainString());
			bmr.setIsCancelled(rate.isCancelled());
			if(rate.getCancellationReason() != null)
				bmr.setCancellationReason(rate.getCancellationReason());
			if(rate.getTimestamp() != null)
				bmr.setTimestamp(Long.toString(rate.getTimestamp().getTime()));
			bmr.setIsExecutable(rate.isExecutable());
			if(rate.getMarketForwardRate() != null)
				bmr.setMarketForwardRate(rate.getMarketForwardRate().toPlainString());
			//Add the built PBMarketRate to the main PBMarketData
			bmd.addBidRates(bmr.build());
		}
		
		for(XStreamRate rate : xrs.getAllAskRates()){
			bmr.clear();
			bmr.setRateId(rate.getRateId());
			if(rate.getSourceId() != null)
				bmr.setSourceId(rate.getSourceId());
			if(rate.getLimit() != null)
				bmr.setLimit(  rate.getLimit().toPlainString());
			if(rate.getMinimumLimit() != null )
				bmr.setMinLimit(rate.getMinimumLimit().toPlainString());
			if(rate.getRate() != null)
				bmr.setRate( rate.getRate().toPlainString());
			if(rate.getSpotRate() != null)
				bmr.setSpotRate(rate.getSpotRate().toPlainString());
			if(rate.getForwardRate() != null)
				bmr.setForwardRate(rate.getForwardRate().toPlainString());
			bmr.setIsCancelled(rate.isCancelled());
			if(rate.getCancellationReason() != null)
				bmr.setCancellationReason(rate.getCancellationReason());
			if(rate.getTimestamp() != null)
				bmr.setTimestamp(Long.toString(rate.getTimestamp().getTime()));
			bmr.setIsExecutable(rate.isExecutable());
			if(rate.getMarketForwardRate() != null)
				bmr.setMarketForwardRate(rate.getMarketForwardRate().toPlainString());
			//Add the built PBMarketRate to the main PBMarketData
			bmd.addAskRates(bmr.build());
		}
		
		
		
		bmdw.setMarketData(bmd.build());
	
		
		return bmdw.build();

	}

}
