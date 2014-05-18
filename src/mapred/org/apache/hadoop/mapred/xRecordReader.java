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
import java.util.ArrayList;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.Seekable;
import org.apache.hadoop.hdfs.DFSClient.DFSDataInputStream;
import org.apache.hadoop.hdfs.DFSClient.DFSInputStream;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CodecPool;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.io.compress.Decompressor;
import org.apache.hadoop.io.compress.SplitCompressionInputStream;
import org.apache.hadoop.io.compress.SplittableCompressionCodec;
import org.apache.hadoop.util.xLog;

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
	private ArrayList<Long> startN = new ArrayList<Long>();
	private ArrayList<CompressionCodecFactory> compressionCodecsN = new ArrayList<CompressionCodecFactory>();
	private ArrayList<CompressionCodec> codecN = new ArrayList<CompressionCodec>();
	private ArrayList<Decompressor> decompressorN = new ArrayList<Decompressor>();

	private int currentRowGroupIndex;

	public static long tweetFileOffset;
	public static String columnsOffsets;

	public long currentOffset = 0;

	private String FIRST_COLUMN_IDENTIFIER;

	private ArrayList<FSDataInputStream> array2inputStreams = new ArrayList<FSDataInputStream>();

	private xFileSplit split;
	private Configuration job;
	private boolean useIndexes = false;

	public static long startTime = 0;

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
		currentRowGroupIndex = 0;
		this.job = job;
		this.split = split;
		startTime = System.currentTimeMillis();
		useIndexes = job.get("useIndexes").equals("true");

		openNextFiles();
	}

	private void openNextFiles() throws IOException {
		if(job.get("jobName").equals("hadoopplusplus")) {
			openHadoopTweetFile();
		} else {
			openRowGroup();
		}
	}

	private void openHadoopTweetFile() throws IOException {
		long currentBlockId = split.getBlocksIds().get(currentRowGroupIndex);
		final Path file = split.getPaths().get(currentRowGroupIndex);		

		if(useIndexes) {
			tweetFileOffset = MapTask.relevantHadoopTweetFile(file.getName(), currentBlockId, job);
		} else {
			tweetFileOffset = 0;
		}

		// open the file and seek to the start of the split
		FileSystem fs = file.getFileSystem(job);
		FSDataInputStream fileIn = fs.open(file);

		xLog.print("xRecordReader: block " + currentBlockId + " relevance: " + tweetFileOffset);
		if(tweetFileOffset == -2) {
			org.apache.hadoop.util.LineReader.conf = job;
			DistributedFileSystem dfs = (DistributedFileSystem) fs;
			tweetFileOffset = dfs.dfs.getOffsetHadoopPlusPlus(split.getLocations()[0], currentBlockId, file.getName());
		}

		if(tweetFileOffset == -3) {
			throw new IOException("mgferreira: offset == -3 ?");
		}
		if(tweetFileOffset != -1) {
			this.maxLineLength = job.getInt("mapred.linerecordreader.maxlength", Integer.MAX_VALUE);

			start = split.getStart();
			end = start + split.getLengths().get(currentRowGroupIndex);

			compressionCodecs = new CompressionCodecFactory(job);
			codec = compressionCodecs.getCodec(file);

			if (isCompressedInput()) {
				decompressor = CodecPool.getDecompressor(codec);
				if (!(codec instanceof SplittableCompressionCodec)) {
					CompressionInputStream is = codec.createInputStream(fileIn, decompressor);
					if(tweetFileOffset > 0) {
						is.skip(tweetFileOffset);
					}
					in = new LineReader(is, job);
					filePosition = fileIn;
				}
			} else {
				fileIn.seek(start);
				in = new LineReader(fileIn, job);
				filePosition = fileIn;
			}
			if (start != 0) {
				start += in.readLine(new Text(), 0, maxBytesToConsume(start));
			}
			this.pos = start;
			System.out.println("start:" + start);
		}
	}

	private void openRowGroup() throws IOException {
		long currentBlockId = split.getBlocksIds().get(currentRowGroupIndex);			
		FIRST_COLUMN_IDENTIFIER = job.get("first.column.identifier");		
		ArrayList<Path> pathsToBlocksOfRelevantSplit = getPathsToRelevantSplit(split.getPaths().get(currentRowGroupIndex), job);
		final Path file = pathsToBlocksOfRelevantSplit.remove(0);

		// open the file and seek to the start of the split
		FileSystem fs = file.getFileSystem(job);
		if(useIndexes) {
			columnsOffsets = MapTask.relevantRowGroup(currentBlockId, file.getName(), job);
		} else {
			columnsOffsets = "0,0";
		}

		array2inputStreams.clear();
		inN.clear();
		posN.clear();

		xLog.print("xRecordReader: block " + currentBlockId + " relevance: " + columnsOffsets);
		if(columnsOffsets.split(",")[0].equals("-2")){
			DistributedFileSystem dfs = (DistributedFileSystem) fs;
			org.apache.hadoop.util.LineReader.conf = job;
			columnsOffsets = dfs.dfs.getOffset(split.getLocations()[0], currentBlockId, file.getName());
		}

		if(!columnsOffsets.split(",")[0].equals("-1")) {
			this.maxLineLength = job.getInt("mapred.linerecordreader.maxlength", Integer.MAX_VALUE);	
			start = split.getStart();
			end = start + split.getLengths().get(currentRowGroupIndex);

			compressionCodecs = new CompressionCodecFactory(job);
			codec = compressionCodecs.getCodec(file);

			FSDataInputStream fileIn = fs.open(file);
			int i = 0;
			for (Path path : pathsToBlocksOfRelevantSplit) {
				array2inputStreams.add(i, fs.open(path));
				startN.add(i, new Long(0));
				CompressionCodecFactory compressionCodecsTmp = new CompressionCodecFactory(job);
				compressionCodecsN.add(i,compressionCodecsTmp);
				codecN.add(i,compressionCodecsTmp.getCodec(path));
				i++;
			}

			if (isCompressedInput()) {
				decompressor = CodecPool.getDecompressor(codec);
				if (!(codec instanceof SplittableCompressionCodec)) {
					CompressionInputStream is = codec.createInputStream(fileIn, decompressor);
					if(!columnsOffsets.split(",")[0].equals("0")) {
						is.skip(Long.parseLong(columnsOffsets.split(",")[0]));
					}
					in = new LineReader(is, job);
					filePosition = fileIn;

					i = 0;
					for (FSDataInputStream fileInN: array2inputStreams) {
						CompressionCodec codecTmp  = codecN.get(i);
						Decompressor decompressorTmp = CodecPool.getDecompressor(codecTmp);
						CompressionInputStream istmp = codecTmp.createInputStream(fileInN, decompressorTmp);
						if(!columnsOffsets.split(",")[1].equals("0")) {
							istmp.skip(Long.parseLong(columnsOffsets.split(",")[1]));
						}
						inN.add(i, new LineReader(istmp, job));
						filePositionN.add(i, fileInN);
						i++;
					}
				}
			} else {
				fileIn.seek(start);
				in = new LineReader(fileIn, job);
				filePosition = fileIn;

				i = 0;
				for (FSDataInputStream fileInN: array2inputStreams) {
					fileInN.seek(startN.get(i));
					inN.add(i, new LineReader(fileInN, job));
					filePositionN.add(i, fileInN);
					i++;
				}
			}
			// If this is not the first split, we always throw away first record
			// because we always (except the last split) read one extra line in
			// next() method.
			if (start != 0) {
				start += in.readLine(new Text(), 0, maxBytesToConsume(start));

				i = 0;
				for (LineReader reader: inN) {
					long startn = startN.remove(i).longValue();
					startn += reader.readLine(new Text(), 0, maxBytesToConsume(startn));
					startN.add(i, new Long(startn));
					i++;
				}
			}
			this.pos = start;
			i = 0;
			for (Long startn : startN) {
				posN.add(i, startn);
				i++;
			}
		}
	}

	private ArrayList<Path> getPathsToRelevantSplit(Path file, Configuration job) {
		ArrayList<Path> paths = new ArrayList<Path>();

		String relevantAttrsS = job.get("relevantAttrs");
		String[] relevantAttrs = relevantAttrsS.split(",");

		for(String relevantAttr : relevantAttrs) {
			Path filePath = file.getParent();
			String fileName = file.getName();
			String newFileName = fileName.replace(FIRST_COLUMN_IDENTIFIER, "_" + relevantAttr + "_");
			Path newPath = new Path(filePath, newFileName);
			if(newFileName.contains(FIRST_COLUMN_IDENTIFIER)) {
				paths.add(0, newPath);
			}
			else {
				paths.add(newPath);
			}
		}
		System.out.println("paths: " + paths.toString());
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
		while(currentRowGroupIndex < split.getNumberOfFiles()) {
			while (getFilePosition() <= end) {
				if(stop()) {
					break;
				}
				key.set(pos);

				int newSize = in.readLine(value, maxLineLength, Math.max(maxBytesToConsume(pos), maxLineLength));
				Text accumulator = new Text(value.toString());

				if (newSize == 0) {
					break;
				}
				int i = 0;
				for (LineReader in1 : inN) {
					Text newValue = new Text();
					long pos1 = posN.get(i);
					int newSize1 = in1.readLine(newValue, maxLineLength, Math.max(maxBytesToConsume(pos1), maxLineLength));
					accumulator.set(accumulator.toString() + ";$;#;" + newValue.toString());

					if (newSize1 != 0) {
						pos1 += newSize1;
						posN.remove(i);
						posN.add(i, new Long(pos1));
					}
					i++;
				}
				String extra = "";
				if(job.get("jobName").equals("sort")){
					extra = split.getPaths().get(currentRowGroupIndex).getName() + ",";
				}
				value.set(extra + accumulator.toString());
				pos += newSize;

				if (newSize < maxLineLength) {
					return true;
				}
				// line too long. try again
				LOG.info("Skipped line of size " + newSize + " at pos " + (pos - newSize));
			}

			close();
			currentRowGroupIndex++;
			if(currentRowGroupIndex == split.getNumberOfFiles()) {
				long endTime = System.currentTimeMillis();
				System.out.println("TIME: "  + (endTime-startTime));
				break;
			}
			openNextFiles();
		}
		return false;
	}

	private boolean stop() {
		if(job.get("jobName").equals("hadoopplusplus")) {
			return tweetFileOffset == -1;
		} else {
			return columnsOffsets.split(",")[0].equals("-1");
		}
	}

	public void close() throws IOException {
		try {
			if (in != null)
				in.close();

			for (LineReader in : inN) {
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
		} finally {
			if (decompressor != null) {
				CodecPool.returnDecompressor(decompressor);
			}
		}
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
}
