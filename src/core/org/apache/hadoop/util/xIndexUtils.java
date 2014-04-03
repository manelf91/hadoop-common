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
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.zip.GZIPInputStream;

public class xIndexUtils {

	private static Long blockIdOfFirstBlock = new Long(0);
	// <attribute nr, <attribute value, blockId>>
	public final static TreeMap<Integer, TreeMap<String, TreeSet<Long>>> index = new TreeMap<Integer, TreeMap<String, TreeSet<Long>>> ();

	//first block of split N -> first block of split N, second block of split N, third block of split N... 
	private static TreeMap<Long, Map<Integer, Long>> block2split = new TreeMap<Long,  Map<Integer, Long>>();

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
						Long blockIdL = new Long(item.blockId);
						boolean first = item.first;
						Integer columnNr = new Integer(item.columnNr);

						ByteArrayOutputStream decompressedData = decompressData(compressedData);

						initializeIndexForCurrentColumn(columnNr);
						if(first) {
							//block2split.put(blockIdL, new HashMap<Integer, Long>());
							blockIdOfFirstBlock = blockIdL;
						}

						ByteArrayInputStream bais = new ByteArrayInputStream(decompressedData.toByteArray());
						BufferedReader br = new BufferedReader(new InputStreamReader(bais, Charset.forName("UTF-8")));

						String entry = "";
						while((entry = br.readLine()) != null) {
							addEntriesToIndex(new String(entry), blockIdL, columnNr);
						}

						//HashMap<Integer, Long> split = (HashMap<Integer, Long>) block2split.get(blockIdOfFirstBlock);
						//split.put(columnNr, blockIdL);

						xLog.print("xIndexUtils: index size:\n" + getIndexSizeStr());
					}
					catch (IOException e) {
						xLog.print(e.toString());
					}
					catch (InterruptedException e) {
						xLog.print(e.toString());
					}
				}
			}
		}
	}

	public static void initializeIndexBuilderThread() {
		indexBuilder = new Thread(new xIndexUtils.IndexBuilder());
		indexBuilder.start();
	}

	private static void initializeIndexForCurrentColumn(Integer columnNr) {
		if(!index.containsKey(columnNr)) {
			TreeMap<String, TreeSet<Long>> currentColumnIndex = new TreeMap<String, TreeSet<Long>>();
			index.put(columnNr, currentColumnIndex);
		}
	}

	private static void addEntriesToIndex(String entry, Long blockId, Integer columnNr) {
		TreeMap<String, TreeSet<Long>> currentColumnIndex = index.get(columnNr);
		TreeSet<Long> blocksForEntry = currentColumnIndex.get(entry);
		if(blocksForEntry == null) {
			blocksForEntry = new TreeSet<Long>();
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
			Long blockIdOfAttrNr = split.get(attrNr);

			TreeSet<Long> relevantBlocks = index.get(attrNr).get(predicate);

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
		for (Integer attr : index.keySet()){
			TreeMap<String, TreeSet<Long>> attrIndex = index.get(attr);

			indexSize += "attribute " + attr.intValue() + " has " + attrIndex.size() + " entries \n";

			try {
				FileOutputStream fout = new FileOutputStream("/home/mgferreira/index.obj");
				DataOutputStream dos = new DataOutputStream(fout);
				ObjectOutputStream oos = new ObjectOutputStream(dos);
				oos.writeObject(attrIndex);
				oos.flush();
				oos.close();
				indexSize += "attribute " + attr.intValue() + " = " + dos.size() + " bytes\n";
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return indexSize;
	}
}