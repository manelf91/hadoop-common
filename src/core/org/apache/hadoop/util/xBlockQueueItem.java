package org.apache.hadoop.util;

import java.io.PipedInputStream;
import java.io.PipedOutputStream;

public class xBlockQueueItem {

	public PipedOutputStream data;
	public PipedInputStream pis;
	public long blockId;
	public boolean first;
	public int columnNr;

	public xBlockQueueItem(long blockId, PipedOutputStream currentCompressedData, PipedInputStream pis, int columnNr, boolean first) {
		this.data = currentCompressedData;
		this.pis = pis;
		this.blockId = blockId;
		this.columnNr = columnNr;
		this.first = first;
	}
}