package com.razor.test;

public class AnotherTestMain {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		System.out.println("Hello World");
		int i = 0;
		for (;i<10;i++){
			System.out.println("i="+i);
			System.out.println("Hello YYYY");
			method1(i);
		}
		System.out.println("Hello WWWWW");
		System.out.println("Hello CCCC");
		System.out.println("Hello tttttt");
	}

	static void method1(int x) {
		System.out.println("x="+x);
	}
	static void method2(int x) {
		System.out.println("x="+x);
	}

	static void method3(int x) {
		System.out.println("x="+x);
	}

}
