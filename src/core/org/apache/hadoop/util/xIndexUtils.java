package org.apache.hadoop.util;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectOutputStream;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.zip.GZIPInputStream;

import org.apache.commons.collections.primitives.ArrayLongList;

public class xIndexUtils {

	private static Long blockIdOfFirstBlock;
	// <attribute nr, <attribute value, blockId>>
	public final static HashMap<Integer, HashMap<String, ArrayList<Long>>> index = new HashMap<Integer, HashMap<String,  ArrayList<Long>>> ();

	//first block of split N -> first block of split N, second block of split N, third block of split N... 
	private static HashMap<Long, HashMap<Integer, Long>> block2split = new HashMap<Long,  HashMap<Integer, Long>>();

	public static BlockingQueue<xBlockQueueItem> queue = new LinkedBlockingQueue<xBlockQueueItem>(200);
	static Thread indexBuilder = null;

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
						xLog.print("Going to add blocknr " + columnNr.intValue() + " to index");

						ByteArrayOutputStream decompressedData = decompressData(compressedData);

						initializeIndexForCurrentColumn(columnNr);
						if(first) {
							block2split.put(blockIdL, new HashMap<Integer, Long>());
							blockIdOfFirstBlock = blockIdL;
						}

						ByteArrayInputStream bais = new ByteArrayInputStream(decompressedData.toByteArray());
						BufferedReader br = new BufferedReader(new InputStreamReader(bais, Charset.forName("UTF-8")));

						String entry = "";
						while((entry = br.readLine()) != null) {
							addEntriesToIndex(new String(entry), blockIdL, columnNr);
						}

						HashMap<Integer, Long> split = block2split.get(blockIdOfFirstBlock);
						split.put(columnNr, blockIdL);
						
						xLog.print("Added blocknr " + columnNr.intValue() + " to index");
						xLog.print("xIndexUtils: index size:\n" + getIndexSizeStr());
					}
					catch (Exception e) {
						xLog.print(e.toString());
					}
				}
			}
		}
	}

	public static void initializeIndexBuilderThread() {
		xLog.print("xIndexUtils: Start running IndexBuilderThread\n");
		indexBuilder = new Thread(new xIndexUtils.IndexBuilder());
		indexBuilder.start();
	}

	private static void initializeIndexForCurrentColumn(Integer columnNr) {
		if(!index.containsKey(columnNr)) {
			HashMap<String,  ArrayList<Long>> currentColumnIndex = new HashMap<String,  ArrayList<Long>>();
			index.put(columnNr, currentColumnIndex);
		}
	}

	private static void addEntriesToIndex(String entry, Long blockId, Integer columnNr) {
		HashMap<String,  ArrayList<Long>> currentColumnIndex = index.get(columnNr);
		 ArrayList<Long> blocksForEntry = currentColumnIndex.get(entry);
		if(blocksForEntry == null) {
			blocksForEntry = new  ArrayList<Long>();
			currentColumnIndex.put(entry, blocksForEntry);
		}
		blocksForEntry.add(blockId);
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

			 ArrayList<Long> relevantBlocks = index.get(attrNr).get(predicate);

			if((relevantBlocks == null) || (!relevantBlocks.contains(blockIdOfAttrNr))) {
				xLog.print("xIndexUtils: The row group " + blockId + " is irrelevant");
				return -1;
			}
		}
		xLog.print("xIndexUtils: The row group " + blockId + " is relevant");
		return 1;
	}

	public static String getIndexSizeStr() {
		String indexSize = "# Attributes: " + index.size() + "\n";
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
			indexSize += "total size: " + dos.size() + " bytes\n";

			for (Integer attr : index.keySet()){
				HashMap<String,  ArrayList<Long>> attrIndex = index.get(attr);

				indexSize += "attribute " + attr.intValue() + " has " + attrIndex.size() + " entries \n";

				fout = new FileOutputStream("index.obj");
				dos = new DataOutputStream(fout);
				oos = new ObjectOutputStream(dos);
				oos.writeObject(attrIndex);
				oos.flush();
				oos.close();
				indexSize += "attribute " + attr.intValue() + " = " + dos.size() + " bytes\n";
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		long end = System.currentTimeMillis();
		indexSize += "time to measure index size: " + (end-startTime) + "milliseconds\n";
		return indexSize;
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