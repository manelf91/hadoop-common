package org.apache.hadoop.util;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.zip.GZIPInputStream;

public class xIndexUtils {

	public static int currentColumnNr = 0;
	private static long blockIdOfFirstBlock = 0;
	public static ByteArrayOutputStream compressedData = new ByteArrayOutputStream();

	// <attribute nr, <attribute value, blockId>>
	public final static TreeMap<Integer, TreeMap<String, TreeSet<Long>>> index = new TreeMap<Integer, TreeMap<String, TreeSet<Long>>> ();
	private static TreeMap<String, TreeSet<Long>> currentColumnIndex = null;

	//first block of split N -> first block of split N, second block of split N, third block of split N... 
	private static TreeMap<Long, Map<Integer, Long>> block2split = new TreeMap<Long,  Map<Integer, Long>>();
	public static boolean first;

	public static class IndexBuilder implements Runnable {
		long blockId;
		public IndexBuilder(long blockId){
			this.blockId = blockId;
		}
		@Override
		public void run() {
			synchronized(index) {
				try {
					compressedData.flush();
					compressedData.close();

					ByteArrayOutputStream compressedDataTmp = compressedData;
					compressedData = new ByteArrayOutputStream();

					ByteArrayOutputStream decompressedData = decompressData(compressedDataTmp);

					Long blockIdL = new Long(blockId);
					initializeIndexForCurrentColumn();
					if(first) {
						blockIdOfFirstBlock = blockId;
						block2split.put(blockIdL, new HashMap<Integer, Long>());
					}

					ByteArrayInputStream bais = new ByteArrayInputStream(decompressedData.toByteArray());
					BufferedReader br = new BufferedReader(new InputStreamReader(bais, Charset.forName("UTF-8")));

					String entry = "";
					while((entry = br.readLine()) != null) {
						addEntriesToIndex(entry, blockIdL);
					}

					HashMap<Integer, Long> split = (HashMap<Integer, Long>) block2split.get(new Long(blockIdOfFirstBlock));
					split.put(new Integer(currentColumnNr), blockIdL);
					currentColumnIndex = null;
				}
				catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	}

	private static void initializeIndexForCurrentColumn() {
		currentColumnIndex = index.get(new Integer(currentColumnNr));
		if(currentColumnIndex == null) {
			currentColumnIndex = new TreeMap<String, TreeSet<Long>>();
			index.put(new Integer(currentColumnNr), currentColumnIndex);
		}
	}

	private static void addEntriesToIndex(String entry, Long blockId) {
		TreeSet<Long> blocksForEntry = currentColumnIndex.get(new String(entry));
		if(blocksForEntry == null) {
			blocksForEntry = new TreeSet<Long>();
			currentColumnIndex.put(new String(entry), blocksForEntry);
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

	public static void addPacket(byte[] pktBuf, int dataOff, int len) throws IOException {
		synchronized(index) {
			compressedData.write(pktBuf, dataOff, len);
		}
	}

	//-1=irrelevant, 1=relevant, 0=non_local_block
	public static int checkIfRelevantRowGroup(TreeMap<Integer, String> filters, long blockId) {
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
			long blockIdOfAttrNr = split.get(attrNr).longValue();

			TreeSet<Long> relevantBlocks = index.get(attrNr).get(predicate);

			if((relevantBlocks == null) || (!relevantBlocks.contains(new Long(blockIdOfAttrNr)))) {
				xLog.print("xIndexUtils: The row group " + blockId + " is irrelevant");
				return -1;
			}
		}
		xLog.print("xIndexUtils: The row group " + blockId + " is relevant");
		return 1;
	}
}