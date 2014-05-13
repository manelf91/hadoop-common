/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.mapred;

import java.io.IOException;
import java.io.DataInput;
import java.io.DataOutput;
import java.util.ArrayList;

import org.apache.hadoop.io.UTF8;
import org.apache.hadoop.fs.Path;

/** A section of an input file.  Returned by {@link
 * InputFormat#getSplits(JobConf, int)} and passed to
 * {@link InputFormat#getRecordReader(InputSplit,JobConf,Reporter)}. 
 */
public class xFileSplit extends org.apache.hadoop.mapreduce.InputSplit 
implements InputSplit {
	private long start;
	private long length;
	private String[] hosts;

	/*mgferreira*/
	private ArrayList<Long> blocksIds = new ArrayList<Long>();
	private ArrayList<Path> files = new ArrayList<Path>();
	private ArrayList<Long> lengths = new ArrayList<Long>();
	private int numberOfFiles;

	xFileSplit() {}

	/** Constructs a split.
	 * @deprecated
	 * @param file the file name
	 * @param start the position of the first byte in the file to process
	 * @param length the number of bytes in the file to process
	 */
	@Deprecated
	public xFileSplit(ArrayList<Path> files, long start, long length, JobConf conf) {
		this(files, start, length, (String[])null);
	}

	/** Constructs a split with host information
	 *
	 * @param file the file name
	 * @param start the position of the first byte in the file to process
	 * @param length the number of bytes in the file to process
	 * @param hosts the list of hosts containing the block, possibly null
	 */
	public xFileSplit(ArrayList<Path> files, long start, long length, String[] hosts) {
		this.files = files;
		this.start = start;
		this.length = length;
		this.hosts = hosts;
	}

	/*mgferreira*/
	public xFileSplit(ArrayList<Long> blocksIds, int numberOfFiles, ArrayList<Path> files, long start, ArrayList<Long> lengths, String[] hosts) {
		this(files, start, 0, hosts);
		this.blocksIds = blocksIds;
		this.numberOfFiles = numberOfFiles;
		this.lengths = lengths;
	}

	/** The position of the first byte in the file to process. */
	public long getStart() { return start; }

	/** The number of bytes in the file to process. */
	public long getLength() { return length; }

	public String toString() { return files.toString() + ":" + start + "+" + length; }

	////////////////////////////////////////////
	// Writable methods
	////////////////////////////////////////////

	public void write(DataOutput out) throws IOException {
		out.writeInt(numberOfFiles);
		for(Path file : files) {
			UTF8.writeString(out, file.toString());
		}		
		out.writeLong(start);
		out.writeLong(length);
		for(Long blockId : blocksIds) {
			out.writeLong(blockId.longValue());
		}
		UTF8.writeString(out, hosts[0]);		
		for(Long length : lengths) {
			out.writeLong(length.longValue());
		}	
	}
	public void readFields(DataInput in) throws IOException {
		numberOfFiles = in.readInt();
		for(int i = 0; i < numberOfFiles; i++) {
			files.add(i, new Path(UTF8.readString(in)));
		}
		start = in.readLong();
		length = in.readLong();
		for(int i = 0; i < numberOfFiles; i++) {
			blocksIds.add(i, new Long(in.readLong()));
		}
		hosts = new String[1];
		hosts[0] = UTF8.readString(in);
		for(int i = 0; i < numberOfFiles; i++) {
			lengths.add(i, new Long(in.readLong()));
		}
	}

	public String[] getLocations() throws IOException {
		if (this.hosts == null) {
			return new String[]{};
		} else {
			return this.hosts;
		}
	}

	/* mgferreira*/
	public ArrayList<Long> getBlocksIds() {
		return blocksIds;
	}

	public ArrayList<Path> getPaths() {
		return files;
	}	

	public int getNumberOfFiles() {
		return numberOfFiles;
	}
	
	public void setPaths(ArrayList<Path> files) {
		this.files = files;
	}

	public void setBlocksIds(ArrayList<Long> blocksIds) {
		this.blocksIds = blocksIds;
	}	
	
	public ArrayList<Long> getLengths() {
		return lengths;
	}
}
