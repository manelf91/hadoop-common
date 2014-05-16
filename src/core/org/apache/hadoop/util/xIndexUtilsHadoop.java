package org.apache.hadoop.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;

import com.google.gson.JsonArray;
import com.google.gson.JsonParser;

public class xIndexUtilsHadoop {

	private static String indexDir = getIndexDir();
	public static ArrayList<Long> blocks = new ArrayList<Long>();
	
	static int z = 0;

	private static HashMap<String, Long> openIndex(String fileName) {
		HashMap<String, Long> index = new HashMap<String, Long>();
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
					System.out.println(indexString);
					String[] pairs = indexString.split(", ");
					for (int k = 0; k < pairs.length; k++) {
						String pair = pairs[k];
						String[] keyValue = pair.split("=");
						index.put(keyValue[0], Long.valueOf(keyValue[1]));
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

	//-1=irrelevant, 1=relevant, 0=non_local_block
	public static long checkIfRelevantHadoopTweetFile(HashMap<Integer, String> filters, String file, Long blockId) {
		if(filters.size() == 0) {
			//xLog.print("xIndexUtils: There are no filters. Block is relevant");
			return 0;
		}
		synchronized (indexDir) {
			//xLog.print("xIndexUtils: Going to check if row group " + blockId + " is relevant");
			if(!blocks.contains(blockId)) {
				System.out.println("no indexes");
				return -2;
			}
			HashMap<String, Long> index = openIndex(file);
			for(Integer attrNr : filters.keySet()) {
				String filter = filters.get(attrNr);
				Long offset = index.get(filter);
				if(offset == null) {
					return -1;
				}
				else {
					return offset.longValue();
				}
			}
			return -3; //when does this happen?
		}
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

	public static String extractAttr(String newEntry) {
		ArrayList<String> listdata1 = new ArrayList<String>();
		JsonParser parser = new JsonParser();
		JsonArray jArray1 = parser.parse(newEntry).getAsJsonArray();
		if (jArray1 != null) { 
			for (int i=0; i < jArray1.size(); i++){ 
				listdata1.add(jArray1.get(i).toString());
			}
		}
		String user = listdata1.get(22);
		String location = "";
		String language = "";
		try {
			String after1 = user.split(", location=")[1];
			location = after1.split(", name=")[0];
		} catch(Exception e) {
			String after1 = user.split("location=")[1];
			location = after1.split(", lang=")[0];
		}
		location = "location:" +  location;

		try {
			String after2 = user.split(", lang=")[1];
			language = after2.split(", listed_count=")[0];
		} catch(Exception e) {
			language = user.split(", lang=")[1];
		}
		language = "lang:" +  language;


		if(language.endsWith("\"")) {
			language = language.substring(0, language.length()-1);
		}
		return language;
	}
}