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
import java.io.InputStream;
import java.lang.management.ManagementFactory;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.Seekable;
import org.apache.hadoop.util.xIndexUtils;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CodecPool;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.Decompressor;
import org.apache.hadoop.io.compress.SplitCompressionInputStream;
import org.apache.hadoop.io.compress.SplittableCompressionCodec;

/* mgferreira*/

public class xRecordReader implements RecordReader<LongWritable, Text> {
	private static final Log LOG = LogFactory.getLog(xRecordReader.class.getName());

	private CompressionCodecFactory compressionCodecs = null;
	private long start;
	private long pos;
	private long end;
	private LineReader in;
	int maxLineLength;
	private Seekable filePosition;
	private CompressionCodec codec;
	private Decompressor decompressor;
	
	private ArrayList<LineReader> inN = new ArrayList<LineReader> ();
	private ArrayList<Seekable> filePositionN = new ArrayList<Seekable>();
	private ArrayList<Long> posN = new ArrayList<Long>();
	private int currentBlockIndex;
	private boolean skipCurrentSplit;

	private String FIRST_COLUMN_IDENTIFIER;
	
	private ArrayList<FSDataInputStream> array2inputStreams = new ArrayList<FSDataInputStream>();

	private xFileSplit split;
	private Configuration job;	
	
	/**
	 * A class that provides a line reader from an input stream.
	 * @deprecated Use {@link org.apache.hadoop.util.LineReader} instead.
	 */
	@Deprecated
	public static class LineReader extends org.apache.hadoop.util.LineReader {
		LineReader(InputStream in) {
			super(in);
		}
		LineReader(InputStream in, int bufferSize) {
			super(in, bufferSize);
		}
		public LineReader(InputStream in, Configuration conf) throws IOException {
			super(in, conf);
		}
	}

	public xRecordReader(Configuration job, xFileSplit split) throws IOException {
		currentBlockIndex = 0;
		this.job = job;
		this.split = split;
		openFile();
	}

	private void openFile() throws IOException {
		long currentBlockId = split.getBlocksIds().get(currentBlockIndex);
		skipCurrentSplit = !MapTask.relevantSplit(currentBlockId);
		
		
		if(!skipCurrentSplit) {
			System.out.println("block " + currentBlockId + " is relevant!");
			this.maxLineLength = job.getInt("mapred.linerecordreader.maxlength", Integer.MAX_VALUE);			
			FIRST_COLUMN_IDENTIFIER = job.get("first.column.identifier");

			start = split.getStart();
			end = start + split.getLength();

			ArrayList<Path> pathsToBlocksOfRelevantSplit = getPathsToRelevantSplit(split.getPaths().get(currentBlockIndex), job);
			final Path file = pathsToBlocksOfRelevantSplit.remove(0);
			
			compressionCodecs = new CompressionCodecFactory(job);
			codec = compressionCodecs.getCodec(file);

			// open the file and seek to the start of the split
			FileSystem fs = file.getFileSystem(job);

			for (Path path : pathsToBlocksOfRelevantSplit) {
				array2inputStreams.add(fs.open(path));
			}
			FSDataInputStream fileIn = fs.open(file);

			if (isCompressedInput()) {
				decompressor = CodecPool.getDecompressor(codec);
				if (codec instanceof SplittableCompressionCodec) {
					final SplitCompressionInputStream cIn =
							((SplittableCompressionCodec)codec).createInputStream(
									fileIn, decompressor, start, end,
									SplittableCompressionCodec.READ_MODE.BYBLOCK);
					in = new LineReader(cIn, job);
					start = cIn.getAdjustedStart();
					end = cIn.getAdjustedEnd();
					filePosition = cIn; // take pos from compressed stream
				} else {
					in = new LineReader(codec.createInputStream(fileIn, decompressor), job);
					filePosition = fileIn;
				}
			} else {
				for (FSDataInputStream fileInN: array2inputStreams) {
					fileInN.seek(start);
					inN.add(new LineReader(fileInN, job));
					filePositionN.add(fileInN);
				}
				fileIn.seek(start);
				in = new LineReader(fileIn, job);
				filePosition = fileIn;
			}
			// If this is not the first split, we always throw away first record
			// because we always (except the last split) read one extra line in
			// next() method.
			if (start != 0) {
				start += in.readLine(new Text(), 0, maxBytesToConsume(start));
			}
			this.pos = start;
			for (Path path : pathsToBlocksOfRelevantSplit) {
				posN.add(new Long(0));
			}
		}
	}

	private ArrayList<Path> getPathsToRelevantSplit(Path file, Configuration job) {
		ArrayList<Path> paths = new ArrayList<Path>();
		
		String relevantAttrsS = job.get("relevantAttrs");
		String[] relevantAttrs = relevantAttrsS.split(";");
		
		for(String relevantAttr : relevantAttrs) {
			Path filePath = file.getParent();
			String fileName = file.getName();
			String newFileName = fileName.replace(FIRST_COLUMN_IDENTIFIER, "_" + relevantAttr + "_");
			Path newPath = new Path(filePath, newFileName);
			paths.add(newPath);
		}
		return paths;
	}

	private boolean isCompressedInput() {
		return (codec != null);
	}

	private int maxBytesToConsume(long pos) {
		return isCompressedInput()
				? Integer.MAX_VALUE
						: (int) Math.min(Integer.MAX_VALUE, end - pos);
	}

	private long getFilePosition() throws IOException {
		long retVal;
		if (isCompressedInput() && null != filePosition) {
			retVal = filePosition.getPos();
		} else {
			retVal = pos;
		}
		return retVal;
	}

	public xRecordReader(InputStream in, long offset, long endOffset,
			int maxLineLength) {
		this.maxLineLength = maxLineLength;
		this.in = new LineReader(in);
		this.start = offset;
		this.pos = offset;
		this.end = endOffset;
		this.filePosition = null;
	}

	public xRecordReader(InputStream in, long offset, long endOffset, 
			Configuration job) 
					throws IOException{
		this.maxLineLength = job.getInt("mapred.linerecordreader.maxlength",
				Integer.MAX_VALUE);
		this.in = new LineReader(in, job);
		this.start = offset;
		this.pos = offset;
		this.end = endOffset;    
		this.filePosition = null;
	}

	public LongWritable createKey() {
		return new LongWritable();
	}

	public Text createValue() {
		return new Text();
	}

	/** Read a line. */
	public synchronized boolean next(LongWritable key, Text value)
			throws IOException {



		// We always read one extra line, which lies outside the upper
		// split limit i.e. (end - 1)
		while (getFilePosition() <= end) {
			
			if(skipCurrentSplit) {
				return false;
			}

			key.set(pos);

			int newSize = in.readLine(value, maxLineLength, Math.max(maxBytesToConsume(pos), maxLineLength));
		    Text accumulator = new Text(value.toString());
			
			for (LineReader in : inN) {
				Text newValue = new Text();
				long pos = posN.get(0);
				newSize = in.readLine(newValue, maxLineLength, Math.max(maxBytesToConsume(pos), maxLineLength));
				accumulator.set(accumulator.toString() + ";" + newValue.toString());

				if (newSize != 0) {
					pos += newSize;
					posN.add(0, new Long(pos));
				}
			}
			value.set(accumulator.toString());

			if (newSize == 0) {
				if ((currentBlockIndex + 1) == split.getNumberOfFiles()) {
					return false;
				}
				//close ins
				currentBlockIndex++;
				openFile();
				continue;
			}

			pos += newSize;
			if (newSize < maxLineLength) {
				return true;
			}
			// line too long. try again
			LOG.info("Skipped line of size " + newSize + " at pos " + (pos - newSize));
		}
		return false;
	}

	/**
	 * Get the progress within the split
	 */
	public float getProgress() throws IOException {
		if (start == end) {
			return 0.0f;
		} else {
			return Math.min(1.0f,
					(getFilePosition() - start) / (float)(end - start));
		}
	}

	public synchronized long getPos() throws IOException {
		return pos;
	}

	public synchronized void close() throws IOException {
		try {
			if (in != null) {
				in.close();
			}
		} finally {
			if (decompressor != null) {
				CodecPool.returnDecompressor(decompressor);
			}
		}
	}
}
