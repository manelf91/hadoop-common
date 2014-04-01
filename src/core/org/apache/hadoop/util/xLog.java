package org.apache.hadoop.util;

public class xLog {

	private static String log = "";

	public static void print(String s) {
		synchronized (log) {
			log += s + '\n';
			System.out.println(s);
		}
	}

	public static String getLog() {
		return log;
	}	
}