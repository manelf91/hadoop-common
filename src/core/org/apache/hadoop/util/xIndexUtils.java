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
	
	private static HashMap<String, String> openIndex(String fileName) {
		HashMap<String, String> index = new HashMap<String, String>();
		try {
			File folder = new File(indexDir);
			File[] listOfFiles = folder.listFiles();			

			for (int i = 0; i < listOfFiles.length; i++) {
				String file = listOfFiles[i].getName();
				if(file.equals(fileName+".index")) {
					FileInputStream fileStream = new FileInputStream(indexDir+fileName+".index");
					InputStreamReader decoder = new InputStreamReader(fileStream, "UTF-8");
					BufferedReader reader = new BufferedReader(decoder);
					String indexString = reader.readLine();
					indexString = indexString.substring(1, indexString.length()-1);
					String[] pairs = indexString.split("], ");
					for (int k = 0; k < pairs.length; k++) {
						String pair = pairs[k];
						String[] keyValue = pair.split("=");

						String key = keyValue[0];
						String value = keyValue[1].substring(1);
						if(k == pairs.length - 1) {
							value = value.substring(0, value.length()-1);
						}
						
						String[] values = value.split(", ");
						String offsets = "";
						for (int j = 0; j < values.length; j++) {
							offsets += values[j] + ",";
						}
						index.put(key, offsets);
					}
					reader.close();
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
			return "0,0";
		}
		synchronized (indexDir) {
			//xLog.print("xIndexUtils: Going to check if row group " + blockId + " is relevant");
			if(!blocks.contains(blockId)) {
				return "-2";
			}
			HashMap<String, String> index = openIndex(file);
			for(Integer attrNr : filters.keySet()) {
				String filter = filters.get(attrNr);
				String offset = index.get(filter);
				if(offset == null) {
					return "-1";
				}
				else {
					return offset;
				}
			}
			//when does this happen?
			return "-3";
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