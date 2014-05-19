package org.apache.hadoop.util;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.math.BigInteger;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.zip.GZIPInputStream;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;

public class xIndexUtils {

	// <attribute nr, <attribute value, blockId>>
	public static HashMap<String, HashMap<String, BitSet>> index = null;

	public static ArrayList<Long> blocks = new ArrayList<Long>();
	private static String indexDir = getIndexDir();

	private static int indexSize = -1;

	public static long mapFunctionTime = 0;

	private static HashMap<Integer, String> previousFilters = new HashMap<Integer, String>();
	private static HashMap<String, String> previousRelFiles = null;

	private static String getIndexDir() {
		try {
			if(InetAddress.getLocalHost().getHostName().contains("manuel")) {
				return "/home/manuelgf/indexDir/";
			}
			else {
				return "/mnt/indexDir/";
			}
		} catch (UnknownHostException e) {
			e.printStackTrace();
			System.out.println(e.getMessage());
		}
		return "/mnt/indexDir";
	}

	private static HashMap<String, HashMap<String, String>> openIndex(String hash) {
		HashMap<String, HashMap<String, String>> index = new HashMap<String, HashMap<String, String>>();
		try {
			File folder = new File(indexDir);
			File[] listOfFiles = folder.listFiles();			

			for (int i = 0; i < listOfFiles.length; i++) {
				String file = listOfFiles[i].getName();
				if(file.equals(hash+".index")) {
					FileInputStream fileStream = new FileInputStream(indexDir+hash+".index");
					InputStreamReader decoder = new InputStreamReader(fileStream, "UTF-8");
					BufferedReader reader = new BufferedReader(decoder);
					String indexString = reader.readLine();
					indexString = indexString.substring(1, indexString.length()-1);
					String[] filters = indexString.split("}, ");
					for (int k = 0; k < filters.length; k++) {
						String pair = filters[k];

						String filter = pair.substring(0, pair.indexOf("="));
						HashMap<String, String> mapForThisFilter = new HashMap<String, String>();

						String files = pair.substring(pair.indexOf("=")+1);
						files = files.substring(1, files.length()-1);
						if(filters.length == 1) {
							files = files.substring(0, files.length()-1);
						}
						String[] filesArr = files.split("], ");	

						for(int j = 0; j < filesArr.length; j++) {
							String fileN = filesArr[j].split("=")[0];
							String offsetN = filesArr[j].split("=")[1].substring(1);
							mapForThisFilter.put(fileN, offsetN);
						}
						index.put(filter, mapForThisFilter);
					}
					reader.close();
					System.out.println(index);
					return index;
				}
			}
		} catch (Exception e) {
			System.out.println(e.getMessage());
			e.printStackTrace();
		}
		return null;
	}

	//-1=irrelevant, 1=relevant, 0=non_local_block
	public static String checkIfRelevantRowGroup(HashMap<Integer, String> filters, String file, long blockId) {
		if(filters.size() == 0) {
			//xLog.print("xIndexUtils: There are no filters. Block is relevant");
			return "0, 0";
		}
		synchronized (indexDir) {
			//xLog.print("xIndexUtils: Going to check if row group " + blockId + " is relevant");
			if(!blocks.contains(blockId)) {
				return "-2";
			}
			HashMap<String, String> relevantFilesForJob = null;
			if(previousRelFiles != null && checkSameJob(previousFilters, filters) == true) {
				relevantFilesForJob = previousRelFiles;
			} else {
				previousFilters = filters;
				relevantFilesForJob = new HashMap<String, String>();

				for(Integer attrNr : filters.keySet()) {
					String filter = filters.get(attrNr);
					String filterHash = toHex(filter.getBytes());
					HashMap<String, HashMap<String, String>> index = openIndex(filterHash);
					relevantFilesForJob = index.get(filter);
				}
				previousRelFiles = relevantFilesForJob;
			}
			if(relevantFilesForJob == null) {
				return "-1";
			}
			String offset = relevantFilesForJob.get(file);
			if(offset == null) {
				return "-1";
			}
			else {
				return offset;
			}
		}
	}

	public static int getIndexSize() {
		if (indexSize == -1) {
			indexSize = calcIndexSize();
		}
		return indexSize;
	}

	@StatisticsAnotation
	public static String getIndexStatistics(){
		return "index size:"+getIndexSize();
	}

	@StatisticsAnotation
	public static String getMapFunctionTimeStatistics(){
		return "time:"+mapFunctionTime;
	}

	public synchronized static void increaseMapTime(long time) {
		mapFunctionTime += time;
	}

	public static int calcIndexSize() {
		/*int size = 0;
		for(ArrayList<String> files : attrNr2Files.values()) {
			for(String fileName : files) {
				File f = new File(indexDir + fileName + ".index");
				System.out.println("size: " + f.length());
				size += f.length();
			}
		}
		return size;*/
		return 0;
	}

	public static boolean checkSameJob(HashMap<Integer, String> filters1, HashMap<Integer, String> filters2){
		if(filters1.size() != filters2.size()) {
			return false;
		}
		for(Integer attr1 : filters1.keySet()) {
			String filter1 = filters1.get(attr1);
			String filter2 = filters2.get(attr1);
			if(!filter1.equals(filter2)) {
				return false;
			}
		}
		return true;
	}

	public static HashMap<Integer, String> buildFiltersMap(Configuration job) {
		HashMap<Integer, String> filtersMap = new HashMap<Integer, String>();

		String indexing = job.get("useIndexes");
		if (indexing.equals("true")) {
			String filters = job.get("filteredAttrs");
			String[] filtersByAttr = filters.split(",");

			for (String filteredAttr : filtersByAttr) {
				String body = job.get("filter" + filteredAttr);
				String filter = body.substring(body.indexOf(" ")+1);

				filtersMap.put(new Integer(filteredAttr), filter);
			}
		}
		return filtersMap;
	}

	public static String toHex(byte[] bytes) {
		BigInteger bi = new BigInteger(1, bytes);
		String completedHash = String.format("%0" + (bytes.length << 1) + "X", bi);
		String hash = completedHash.substring(completedHash.length()-2, completedHash.length());
		return hash;
	}
}