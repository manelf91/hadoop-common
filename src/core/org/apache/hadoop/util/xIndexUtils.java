package org.apache.hadoop.util;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectOutputStream;
import java.math.BigInteger;
import java.nio.charset.Charset;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.zip.GZIPInputStream;

import org.apache.commons.collections.primitives.ArrayLongList;

public class xIndexUtils {

	private static Long blockIdOfFirstBlock;
	// <attribute nr, <attribute value, blockId>>
	public final static HashMap<Integer, HashMap<String, BitSet>> index = new HashMap<Integer, HashMap<String,  BitSet>>();

	//first block of split N -> first block of split N, second block of split N, third block of split N... 
	private static HashMap<Long, HashMap<Integer, Long>> block2split = new HashMap<Long,  HashMap<Integer, Long>>();
	private static HashMap<Integer, HashMap<Long, Integer>> blockId2BlockNr = new HashMap<Integer, HashMap<Long, Integer>>();
	private static HashMap<Integer, Integer> blockCnt = new HashMap<Integer, Integer>();

	public static BlockingQueue<xBlockQueueItem> queue = new LinkedBlockingQueue<xBlockQueueItem>(200);
	static Thread indexBuilder = null;

	public static int nBlocks = 0;

	public static class IndexBuilder implements Runnable {

		@Override
		public void run() {
			while(true) {
				synchronized(index) {
					try {
						xBlockQueueItem item = queue.take();

						ByteArrayOutputStream compressedData = item.data;
						long blockId =item.blockId;
						Long blockIdL = new Long(blockId);
						boolean first = item.first;
						Integer columnNr = new Integer(item.columnNr);
						xLog.print("Going to add columnNr " + columnNr.intValue() + " to index");

						int blocknr = updateBlockMap(blockIdL, columnNr);

						ByteArrayOutputStream decompressedData = decompressData(compressedData);

						initializeIndexForCurrentColumn(columnNr);
						if(first) {
							block2split.put(blockIdL, new HashMap<Integer, Long>());
							blockIdOfFirstBlock = blockIdL;
							nBlocks++;
						}

						ByteArrayInputStream bais = new ByteArrayInputStream(decompressedData.toByteArray());
						BufferedReader br = new BufferedReader(new InputStreamReader(bais, Charset.forName("UTF-8")));

						String entry = "";
						while((entry = br.readLine()) != null) {
							String charset = "UTF-8";
							String newEntry = new String(entry.getBytes(), charset);
							int lengthBefore = newEntry.getBytes(charset).length;

							MessageDigest messageDigest = MessageDigest.getInstance("SHA-256");
							byte[] hash = messageDigest.digest(newEntry.getBytes(charset));
					        String hashedNewEntry = new String(hash, charset);
							int lengthAfter = hashedNewEntry.getBytes(charset).length;

							if(lengthBefore > lengthAfter) {
								addEntriesToIndex(hashedNewEntry, blocknr, columnNr);
							}
							else {
								addEntriesToIndex(newEntry, blocknr, columnNr);
							}
						}

						HashMap<Integer, Long> split = block2split.get(blockIdOfFirstBlock);
						split.put(columnNr, blockIdL);

						xLog.print("Added columnNr " + columnNr.intValue() + " to index");
						if((nBlocks == 28 || nBlocks == 14 || nBlocks == 7) && columnNr.intValue() == 1) {
							//printIndexSize();
							//removeIndexEntriesWithMoreThan(14);
							printIndexSize();
						}
					}
					catch (Exception e) {
						e.printStackTrace();
					}
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

	private static void initializeIndexForCurrentColumn(Integer columnNr) {
		if(!index.containsKey(columnNr)) {
			HashMap<String,  BitSet> currentColumnIndex = new HashMap<String,  BitSet>();
			index.put(columnNr, currentColumnIndex);
		}
	}

	private static void addEntriesToIndex(String hash, int blocknr, Integer columnNr) {
		HashMap<String,  BitSet> currentColumnIndex = index.get(columnNr);
		BitSet blocksForEntry = currentColumnIndex.get(hash);
		if(blocksForEntry == null) {
			blocksForEntry = new  BitSet(28);
			currentColumnIndex.put(hash, blocksForEntry);
		}
		blocksForEntry.set(blocknr);
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
		xLog.print("xIndexUtils: Going to check if row group " + blockId + " is relevant");
		if(filters.size() == 0)
			return 1;

		HashMap<Integer, Long> split = (HashMap<Integer, Long>) block2split.get(new Long(blockId));

		if(split == null) {
			xLog.print("xIndexUtils: Reading a non-local row group: " + blockId);
			return 0;
		}

		for(Integer attrNr : filters.keySet()) {
			String predicate = filters.get(attrNr);
			Long blockIdOfAttrNr = split.get(attrNr);
			Integer blocknr = blockId2BlockNr.get(attrNr).get(blockIdOfAttrNr);

			BitSet relevantBlocks = index.get(attrNr).get(predicate);

			if((relevantBlocks == null) || (relevantBlocks.get(blocknr.intValue()) == false )) {
				xLog.print("xIndexUtils: The row group " + blockId + " is irrelevant");
				return -1;
			}
		}
		xLog.print("xIndexUtils: The row group " + blockId + " is relevant");
		return 1;
	}

	public static void printIndexSize() {
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
				/*for (String entry : attrIndex.keySet()) {
					ArrayList<Long> blockList = attrIndex.get(entry);
					System.out.println("[i1] " + attr + "_s" + i + ": " +  blockList.size());
					System.out.println("[i1] !!!" + entry + " !!!SIZE:" + blockList.size() + "!!!");
					i++;
				}*/
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
	}

	public static HashMap<Integer, String> buildFiltersMap(String filters) {
		HashMap<Integer, String> filtersMap = new HashMap<Integer, String>();
		// <attribute number #>-<predicate>;<attribute number #>-<predicate>...
		if(filters != null) {
			String[] filtersArr = filters.split(",");
			for (String filter : filtersArr) {
				Integer attrNr = Integer.parseInt(filter.split("-")[0]);
				String attrValue = filter.split("-")[1];
				filtersMap.put(attrNr, attrValue);
			}
		}
		return filtersMap;
	}
}