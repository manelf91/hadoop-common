package org.apache.hadoop.util;

import java.io.ByteArrayOutputStream;

public class xBlockQueueItem {

	public ByteArrayOutputStream data;
	public long blockId;
	public boolean first;
	public int columnNr;

	public xBlockQueueItem(long blockId, ByteArrayOutputStream currentCompressedData, int columnNr, boolean first) {
		this.data = currentCompressedData;
		this.blockId = blockId;
		this.columnNr = columnNr;
		this.first = first;
	}
}