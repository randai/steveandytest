package com.razor.test;
//Comment 1 - make a conflict
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.ObjectInputStream;

import com.financialogix.xstream.common.data.XStreamRates;

public class AnotherTestMain {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		try
		{
			FileInputStream fis = new FileInputStream("/Users/andy/tmp/streamRate.obj");
			ObjectInputStream ois = new ObjectInputStream(fis);
			XStreamRates rates = (XStreamRates)ois.readObject();
			ois.close();
		}
		catch (FileNotFoundException e)
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		catch (IOException e)
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		catch (ClassNotFoundException e)
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
		
	}

}
