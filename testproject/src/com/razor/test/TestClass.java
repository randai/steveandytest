package com.razor.test;

public class TestClass
{

	/**
	 * @param args
	 */
	public static void main(String[] args)
	{
		String fullSubject = "10000.123";
		String [] subjectParts = fullSubject.split("\\.");
    	String actualSubject = fullSubject.substring(0,fullSubject.length()-(subjectParts[subjectParts.length-1].length()+1) );
System.out.println(actualSubject);

	}

}
