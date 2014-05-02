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

	//first block of split N -> first block of split N, second block of split N, third block of split N... 
	private static HashMap<Long, HashMap<Integer, Long>> block2split = new HashMap<Long,  HashMap<Integer, Long>>();

	private static HashMap<Integer, HashMap<Long, Integer>> blockId2BlockNr = new HashMap<Integer, HashMap<Long, Integer>>();
	private static HashMap<Integer, Integer> blockCnt = new HashMap<Integer, Integer>();

	private static HashMap<Integer, ArrayList<String>> attrNr2Files = new HashMap<Integer, ArrayList<String>>();

	public static BlockingQueue<xBlockQueueItem> queue = new LinkedBlockingQueue<xBlockQueueItem>(200);
	static Thread indexBuilder = null;

	private static Long blockIdOfFirstBlock;
	public static int nBlocks = 0;

	private static String indexDir = getIndexDir();

	private static HashMap<Integer, String> previousFilters = new HashMap<Integer, String>();
	private static HashMap<Integer, BitSet> previousRelBlocks = null;

	private static int indexSize = -1;

	static {
		Object obj = new xIndexUtils();
		StatisticsAgregator.getInstance().register(obj);
	}

	public static class IndexBuilder implements Runnable {

		@Override
		public void run() {
			while(true) {
				try {
					xBlockQueueItem item = queue.take();

					ByteArrayOutputStream compressedData = item.data;
					long blockId =item.blockId;
					Long blockIdL = new Long(blockId);
					boolean first = item.first;
					Integer columnNr = new Integer(item.columnNr);
					xLog.print("Going to add columnNr " + columnNr.intValue() + " to index");

					openIndexFiles(columnNr);

					int blocknr = updateBlockMap(blockIdL, columnNr);

					ByteArrayOutputStream decompressedData = decompressData(compressedData);

					if(first) {
						block2split.put(blockIdL, new HashMap<Integer, Long>());
						blockIdOfFirstBlock = blockIdL;
						nBlocks++;
					}

					ByteArrayInputStream bais = new ByteArrayInputStream(decompressedData.toByteArray());
					BufferedReader br = new BufferedReader(new InputStreamReader(bais, Charset.forName("UTF-8")));

					String entry = "";
					while((entry = br.readLine()) != null) {
						String newEntry = new String(entry);
						addEntriesToIndex(newEntry, blocknr);
					}

					HashMap<Integer, Long> split = block2split.get(blockIdOfFirstBlock);
					split.put(columnNr, blockIdL);
					xLog.print("Added columnNr " + columnNr.intValue() + " to index");

					closeIndexFiles(columnNr);
				}
				catch (Exception e) {
					e.printStackTrace();
					System.out.println(e.getMessage());
				}
			}
		}
	}

	public static int updateBlockMap(Long blockIdL, Integer columnNr) {
		Integer blockCnt4ColumnNr = blockCnt.get(columnNr);
		if(blockCnt4ColumnNr == null) {
			blockCnt4ColumnNr = new Integer(0);
		}
		else {
			int currentCnt = blockCnt4ColumnNr.intValue();
			blockCnt4ColumnNr = new Integer(currentCnt + 1);
		}
		blockCnt.put(columnNr, blockCnt4ColumnNr);

		HashMap<Long, Integer> attrMap = blockId2BlockNr.get(columnNr);
		if(attrMap == null) {
			attrMap = new HashMap<Long, Integer>();
			blockId2BlockNr.put(columnNr, attrMap);
		}
		attrMap.put(blockIdL, blockCnt4ColumnNr);
		return blockCnt4ColumnNr.intValue();
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

	public static void closeIndexFiles(Integer columnNr) {
		ArrayList<String> filesList = new ArrayList<String>();
		FileOutputStream fout;
		DataOutputStream dos;
		ObjectOutputStream oos;
		try {
			for(String hash : index.keySet()) {
				HashMap<String, BitSet> attrIndex = index.get(hash);
				filesList.add(hash + "-" + columnNr);
				fout = new FileOutputStream(indexDir + hash + "-" + columnNr + ".index");
				dos = new DataOutputStream(fout);
				oos = new ObjectOutputStream(dos);
				oos.writeObject(attrIndex);
				oos.flush();
				oos.close();
			}
		} catch(Exception e) {
			e.printStackTrace();
			System.out.println(e.getMessage());
		}
		attrNr2Files.put(columnNr, filesList);
		index = new HashMap<String, HashMap<String,BitSet>>();
	}

	public static void openIndexFiles(Integer columnNr) {
		if(index == null) {
			removeAllPreviousIndexFiles();
			index = new HashMap<String, HashMap<String,BitSet>>();
		}
		ArrayList<String> filesList = attrNr2Files.get(columnNr);
		if(filesList == null) {
			return;
		}
		FileInputStream fin;
		ObjectInputStream ois;
		try {
			for (String file : filesList) {
				fin = new FileInputStream(indexDir + file + ".index");
				ois = new ObjectInputStream(fin);
				HashMap<String, BitSet> attrIndex = (HashMap<String, BitSet>) ois.readObject();
				index.put(file.split("-")[0], attrIndex);
				fin.close();
			}
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println(e.getMessage());
		}
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
		xLog.print("xIndexUtils: Start running IndexBuilderThread\n");
		indexBuilder = new Thread(new xIndexUtils.IndexBuilder());
		indexBuilder.start();
	}

	/*public static void removeIndexEntriesWithMoreThan(int i) {
		int countRemoved = 0;
		for (Integer attr : index.keySet()){
			HashMap<String,  ArrayList<Long>> attrIndex = index.get(attr);
			Iterator<Map.Entry<String,  ArrayList<Long>>> iter = attrIndex.entrySet().iterator();
			while (iter.hasNext()) {
				Map.Entry<String,  ArrayList<Long>> entry = iter.next();
				ArrayList<Long> blockList = entry.getValue();
				if(blockList.size() >= i) {
					System.out.println("Attr :" + attr + " removed " + entry.getKey());
					iter.remove();
					countRemoved++;
				}
			}
			System.out.println("for attr " + attr + " were removed " + countRemoved + " items");
			countRemoved = 0;
		}
	}*/

	private static void addEntriesToIndex(String entry, int blocknr) {
		try {
			String hash = toHex(entry.getBytes());
			HashMap<String,  BitSet> currentColumnIndex = index.get(hash);
			if(currentColumnIndex == null) {
				currentColumnIndex = new HashMap<String, BitSet>();
				index.put(hash, currentColumnIndex);
			}
			BitSet blocksForEntry = currentColumnIndex.get(entry);
			if(blocksForEntry == null) {
				blocksForEntry = new  BitSet(28);
				currentColumnIndex.put(entry, blocksForEntry);
			}
			blocksForEntry.set(blocknr);
		} catch(Exception e) {
			e.printStackTrace();
			System.out.println("entry: " + entry);
			System.out.println(e.getMessage());
		}
	}

	public static ByteArrayOutputStream decompressData(ByteArrayOutputStream compressedData) throws IOException {
		ByteArrayOutputStream decompressedData = new ByteArrayOutputStream();
		ByteArrayInputStream bais = new ByteArrayInputStream(compressedData.toByteArray());
		GZIPInputStream gZIPInputStream = new GZIPInputStream(bais);

		int bytes_read;
		byte[] buffer = new byte[1024];

		while ((bytes_read = gZIPInputStream.read(buffer)) > 0) {
			decompressedData.write(buffer, 0, bytes_read);
		}

		gZIPInputStream.close();
		decompressedData.close();
		return decompressedData;
	}

	//-1=irrelevant, 1=relevant, 0=non_local_block
	public static int checkIfRelevantRowGroup(HashMap<Integer, String> filters, long blockId) {
		if(filters.size() == 0) {
			xLog.print("xIndexUtils: There are no filters. Block is relevant");
			return 1;
		}
		synchronized (index) {
			xLog.print("xIndexUtils: Going to check if row group " + blockId + " is relevant");

			HashMap<Integer, BitSet> relevantBlocksForJob = null;
			HashMap<Integer, Long> split = (HashMap<Integer, Long>) block2split.get(new Long(blockId));

			if(previousRelBlocks != null && checkSameJob(previousFilters, filters) == true) {
				relevantBlocksForJob = previousRelBlocks;
			} else {
				previousFilters = filters;
				relevantBlocksForJob = new HashMap<Integer, BitSet>();

				for(Integer attrNr : filters.keySet()) {
					try{
						openIndexFiles(attrNr);
						String filter = filters.get(attrNr);
						String filterHash = toHex(filter.getBytes());
						BitSet relevantBlocksForThisFilter = index.get(filterHash).get(filter);
						relevantBlocksForJob.put(attrNr, relevantBlocksForThisFilter);
					} catch(Exception e) {
						e.printStackTrace();
						System.out.println(e.getMessage());
					}
				}
				previousRelBlocks = relevantBlocksForJob;
			}

			if(split == null) {
				xLog.print("xIndexUtils: Reading a non-local row group: " + blockId);
				return 0;
			}
			for(Integer attrNr : filters.keySet()) {
				Long blockIdOfAttrNr = split.get(attrNr);
				Integer blocknr = blockId2BlockNr.get(attrNr).get(blockIdOfAttrNr);
				BitSet relevantBlocks = relevantBlocksForJob.get(attrNr);

				if((relevantBlocks == null) || (relevantBlocks.get(blocknr.intValue()) == false )) {
					xLog.print("xIndexUtils: The row group " + blockId + " is irrelevant");
					return -1;
				}
			}

			xLog.print("xIndexUtils: The row group " + blockId + " is relevant");
			return 1;
		}
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

	public static int getIndexSize() {
		if (indexSize == -1) {
			indexSize = calcIndexSize();
		}
		return indexSize;
	}

	public static int calcIndexSize(){
		return 0;
	}

	@StatisticsAnotation
	public static String getIndexStatistics(){
		return "index size:"+getIndexSize();
	}

	/*public static int calcIndexSize() {
		int size = 0;
		System.out.println("[i1] # Attributes: " + index.size());
		long startTime = System.currentTimeMillis();
		FileOutputStream fout;
		DataOutputStream dos;
		ObjectOutputStream oos;
		try {
			fout = new FileOutputStream("index.obj");
			dos = new DataOutputStream(fout);
			oos = new ObjectOutputStream(dos);
			oos.writeObject(index);
			oos.flush();
			oos.close();
			System.out.println("[i1] total size: " + dos.size() + " bytes");

			for (Integer attr : index.keySet()){
				HashMap<String,  BitSet> attrIndex = index.get(attr);
				System.out.println("[i1] attribute " + attr.intValue() + " has " + attrIndex.size() + " entries");
				int i = 0;
				fout = new FileOutputStream("index.obj");
				dos = new DataOutputStream(fout);
				oos = new ObjectOutputStream(dos);
				oos.writeObject(attrIndex);
				oos.flush();
				oos.close();
				System.out.println("[i1] attribute " + attr.intValue() + " = " + dos.size() + " bytes");
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		long end = System.currentTimeMillis();
		System.out.println("[i1] time to measure index size: " + (end-startTime) + "milliseconds");
	}*/

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
	    return String.format("%0" + (bytes.length << 1) + "X", bi).substring(0, 2);
	}
}