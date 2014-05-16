package org.apache.hadoop.util;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.zip.GZIPInputStream;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;

import com.google.gson.JsonArray;
import com.google.gson.JsonParser;

public class xIndexUtilsHadoop {

	// <<attribute value, offset>>
	public static HashMap<String, Long> index = null;	
	public static ArrayList<Long> blocks = new ArrayList<Long>();

	public static BlockingQueue<xBlockQueueItem> queue = new LinkedBlockingQueue<xBlockQueueItem>(10);

	static Thread indexBuilder = null;

	private static String indexDir = getIndexDir();
	private static int indexSize = -1;
	
	public static long currentOffset = 0;


	static {
		Object obj = new xIndexUtilsHadoop();
		StatisticsAgregator.getInstance().register(obj);
	}

	public static class IndexBuilder implements Runnable {

		@Override
		public void run() {
			while(true) {
				try {
					xBlockQueueItem item = queue.take();
					Long blockId = new Long(item.blockId);
					blocks.add(blockId);
					currentOffset = 0;
					index = new HashMap<String, Long>(); 

					PipedOutputStream pos = item.data;

					PipedInputStream pis = item.pis;
					GZIPInputStream gzis = new GZIPInputStream(pis);
					BufferedReader br = new BufferedReader(new InputStreamReader(gzis, Charset.forName("UTF-8")));

					
					String entry = "";
					while((entry = br.readLine()) != null) {
						String newEntry = new String(entry);
						String attr = extractAttr(newEntry);
						addEntriesToIndex(attr);
						currentOffset += (newEntry.getBytes(Charset.forName("UTF-8")).length + 1);
					}
					br.close();
					gzis.close();
					pis.close();
					pos.close();
					writeIndex(blockId.toString());
				}
				catch (Exception e) {
					e.printStackTrace();
					System.out.println(e.getMessage());
				}
			}
		}
	}

	private static void writeIndex(String blockId) {
		System.out.println("index:");
		System.out.println(index);
		try {
			FileOutputStream fout = new FileOutputStream(indexDir + blockId + ".index");
			DataOutputStream dos = new DataOutputStream(fout);
			ObjectOutputStream oos = new ObjectOutputStream(dos);
			oos.writeObject(index);
			oos.flush();
			oos.close();
		} catch (Exception e) {
			System.out.println(e.getMessage());
			e.printStackTrace();
		}
	}

	private static void openIndex(Long blockId) {
		try {
			FileInputStream fin = new FileInputStream(indexDir + blockId + ".index");
			ObjectInputStream ois = new ObjectInputStream(fin);
			index = (HashMap<String, Long>) ois.readObject();
			fin.close();
		} catch (Exception e) {
			System.out.println(e.getMessage());
			e.printStackTrace();
		}
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

	private static void removeAllPreviousIndexFiles() {
		try {
			FileUtils.cleanDirectory(new File(indexDir));
		} catch (IOException e) {
			e.printStackTrace();
			System.out.println(e.getMessage());
		}
	}

	public static void initializeIndexBuilderThread() {
		xLog.print("xIndexUtilsHadoop: Start running IndexBuilderThread\n");
		indexBuilder = new Thread(new xIndexUtilsHadoop.IndexBuilder());
		indexBuilder.start();
		removeAllPreviousIndexFiles();
	}

	private static void addEntriesToIndex(String entry) {
		try {			
			if (!index.containsKey(entry)) {
				index.put(entry, currentOffset);
			}
		} catch(Exception e) {
			e.printStackTrace();
			System.out.println("entry: " + entry);
			System.out.println(e.getMessage());
		}
	}

	public static ByteArrayOutputStream decompressData(ByteArrayOutputStream compressedData) throws IOException {
		ByteArrayOutputStream decompressedData = new ByteArrayOutputStream();
		ByteArrayInputStream bais = new ByteArrayInputStream(compressedData.toByteArray());
		GZIPInputStream gzis = new GZIPInputStream(bais);
		BufferedReader br = new BufferedReader(new InputStreamReader(gzis, Charset.forName("UTF-8")));
	

		int bytes_read;
		byte[] buffer = new byte[1024];

		while ((bytes_read = gzis.read(buffer)) > 0) {
			decompressedData.write(buffer, 0, bytes_read);
		}

		gzis.close();
		decompressedData.close();
		return decompressedData;
	}

	//-1=irrelevant, 1=relevant, 0=non_local_block
	public static long checkIfRelevantHadoopTweetFile(HashMap<Integer, String> filters, Long blockId) {
		if(filters.size() == 0) {
			//xLog.print("xIndexUtils: There are no filters. Block is relevant");
			return 0;
		}
		if(!blocks.contains(blockId)) {
			//xLog.print("xIndexUtilsHadoop: Remote block");
			return -2;
		}
		synchronized (index) {
			//xLog.print("xIndexUtils: Going to check if row group " + blockId + " is relevant");
			openIndex(blockId);
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

	public static int calcIndexSize() {
		int size = 0;
			for(Long blockId : blocks) {
				File f = new File(indexDir + blockId + ".index");
				size += f.length();
		}
		return size;
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