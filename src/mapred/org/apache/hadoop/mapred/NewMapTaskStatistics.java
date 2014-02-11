package org.apache.hadoop.mapred;

public class NewMapTaskStatistics {
	static long timeTaken=0;
	
	public static String method1() {
		return "Stat 1:Value 1";
	}

	
	@Statistics
	public static String timeTaken(){
		return "Time Taken:"+timeTaken;
	}
	
	@Statistics
	public static String filesCombined(){
		return "Files Combined:"+30;
	}
	}


