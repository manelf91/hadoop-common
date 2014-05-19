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

package org.apache.hadoop.examples;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.math.BigInteger;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.MapTask;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapred.lib.MultipleOutputs;
import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat;
import org.apache.hadoop.mapred.lib.xInputFormat;
import org.apache.hadoop.mapred.Partitioner;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.google.gson.JsonArray;
import com.google.gson.JsonParser;

/**
 * This is an example Hadoop Map/Reduce application.
 * It reads the text input files, breaks each line into words
 * and counts them. The output is a locally sorted list of words and the 
 * count of how often they occurred.
 *
 * To run: bin/hadoop jar build/hadoop-examples.jar wordcount
 *            [-m <i>maps</i>] [-r <i>reduces</i>] <i>in-dir</i> <i>out-dir</i> 
 */
public class CreateIndexesMy extends Configured implements Tool {

	/**
	 * Counts the words in each line.
	 * For each line of input, break the line into words and emit them as
	 * (<b>word</b>, <b>1</b>).
	 */
	public static class MapClass extends MapReduceBase
	implements Mapper<LongWritable, Text, Text, Text> {

		private HashMap<String, ArrayList<Long>> offsets = new HashMap<String, ArrayList<Long>>();
		private long currentOffsetLang = 0;
		private long currentOffsetText = 0;
		OutputCollector<Text, Text> output;
		String fileName = "";
		String node = "";

		public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
			this.output = output;
			String line = value.toString();
			if(this.fileName.equals("")) {
				String fileName = new String(line.substring(line.indexOf(";")+1, line.indexOf(",")));
				this.fileName = fileName;
			}
			if(this.node.equals("")) {
				String node = new String(line.substring(0, line.indexOf(";")));
				this.node = node;
			}
			String language = new String(line.substring(line.indexOf(",")+1, line.indexOf(";$;#;")));
			String text = new String(line.substring(line.indexOf(";$;#;")+5));

			if(!offsets.containsKey(language)) {				
				ArrayList<Long> offsetsForThisEntry = new ArrayList<Long>();
				offsetsForThisEntry.add(currentOffsetLang);
				offsetsForThisEntry.add(currentOffsetText);
				offsets.put(language, offsetsForThisEntry);
			}
			currentOffsetLang += (language.getBytes(Charset.forName("UTF-8")).length + 1);
			currentOffsetText += (text.getBytes(Charset.forName("UTF-8")).length + 1);
		}

		@Override
		public void close() throws IOException {
			for(String lang : offsets.keySet()) {
				ArrayList<Long> offsetsForThisEntry = offsets.get(lang);
				output.collect(new Text(node), new Text(fileName + "," + lang + ";" + offsetsForThisEntry.toString()));
			}
		}
	}

	/**
	 * A reducer class that just emits the sum of the input values.
	 */
	public static class Reduce extends MapReduceBase
	implements Reducer<Text, Text, Text, Text> {
		private MultipleOutputs mos;
		private String node;
		private Reporter reporter;

		//hash(lang), <lang, <file, offsets>>
		HashMap<String, HashMap<String, HashMap<String, String>>> offsetsMap = new HashMap<String, HashMap<String, HashMap<String, String>>>();

		
		public void configure(JobConf conf) {
			mos = new MultipleOutputs(conf);
		}
		
		public static String toHex(byte[] bytes) {
			BigInteger bi = new BigInteger(1, bytes);
			String completedHash = String.format("%0" + (bytes.length << 1) + "X", bi);
			String hash = completedHash.substring(completedHash.length()-2, completedHash.length());
			return hash;
		}

		public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, 
				Reporter reporter) throws IOException {
			this.node = key.toString();
			if(this.node.contains("manuelgf")) {
				this.node = "manuelgf";
			} else {
				this.node = node.substring(0, node.indexOf("."));
			}
			this.reporter = reporter;
			
			while (values.hasNext()) {
				String value = values.next().toString();
				String fileName = value.substring(0, value.indexOf(","));
				String lang = value.substring(value.indexOf(",")+1, value.indexOf(";"));
				String hashLang = toHex(lang.getBytes());
				String offsets = value.substring(value.indexOf(";")+1);
				
				HashMap<String, HashMap<String, String>> offsetsForThisHash = offsetsMap.get(hashLang);
				if(offsetsForThisHash == null) {
					offsetsForThisHash = new HashMap<String, HashMap<String, String>>();
					offsetsMap.put(hashLang, offsetsForThisHash);
				}
				HashMap<String, String> offsetsForThisEntry = offsetsForThisHash.get(lang);
				if(offsetsForThisEntry == null) {
					offsetsForThisEntry = new HashMap<String, String>();
					offsetsForThisHash.put(lang, offsetsForThisEntry);
				}
				offsetsForThisEntry.put(fileName, offsets);
			}
		}
		public void close() throws IOException {
			for(String hash : offsetsMap.keySet()) {
				HashMap<String, HashMap<String, String>> offsetsForThisHash = offsetsMap.get(hash);
				mos.getCollector("index", node + hash + "index", reporter).collect(new Text(offsetsForThisHash.toString()), NullWritable.get());
			}
			mos.close();
		}
	}

	public static class MyPartitioner implements Partitioner<Text, Text> {	
		HashMap<String, Integer> partitions = new HashMap<String, Integer>();

		public MyPartitioner(){}

		@Override
		public void configure(JobConf job) {
			String nodes = job.get("nodes");
			String[] nodeList = nodes.split(",");
			int i = 0;
			for(String node : nodeList) {
				partitions.put(node, new Integer(i));
				i++;
			}
			System.out.println(partitions.toString());
		}

		@Override
		public int getPartition(Text key, Text value, int numPartitions) {
			String node = key.toString();
			return partitions.get(node);
		}
	}

	static int printUsage() {
		System.out.println("wordcount [-m <maps>] [-r <reduces>] <input> <output>");
		ToolRunner.printGenericCommandUsage(System.out);
		return -1;
	}

	/**
	 * The main driver for word count map/reduce program.
	 * Invoke this method to submit the map/reduce job.
	 * @throws IOException When there is communication problems with the 
	 *                     job tracker.
	 */
	public int run(String[] args) throws Exception {
		JobConf conf = new JobConf(getConf(), CreateIndexesMy.class);

		/*mgferreira*/

		conf.setIfUnset("useIndexes", "false");
		conf.setBooleanIfUnset("equal.splits", true);
		conf.setIfUnset("blocks.per.split", "1");

		String relevantAttrs = "";
		String filteredAttrs = "";

		BufferedReader br1 = new BufferedReader(new FileReader("names"));
		String node;
		String nodes = "";
		while ((node = br1.readLine()) != null) {
			nodes += node + ",";
		}
		conf.setIfUnset("nodes", nodes);

		BufferedReader br = new BufferedReader(new FileReader(args[1]));
		String pred;
		int n = 0;
		while ((pred = br.readLine()) != null) {
			String absAttr = "";
			if(!pred.contains(" ")){
				absAttr = pred;
			}
			else {
				int indexOfFirstSpace = pred.indexOf(" ");
				int indexOfSecondSpace = pred.indexOf(" ",indexOfFirstSpace+1);

				absAttr = new String(pred.substring(0, indexOfFirstSpace));
				String relAttr = new String(pred.substring(indexOfFirstSpace+1, indexOfSecondSpace));
				filteredAttrs += relAttr + ",";
				String filter = new String(pred.substring(indexOfSecondSpace+1));
				System.out.println("filter" + relAttr +": " + n + " " + filter);
				conf.setIfUnset("filter" + relAttr, n + " " + filter);
			}
			relevantAttrs += absAttr + ",";
			n++;
		}
		br.close();

		System.out.println("relevantAttrs: " + relevantAttrs);
		System.out.println("filteredAttrs: " + filteredAttrs);
		conf.setIfUnset("relevantAttrs", relevantAttrs);
		conf.setJobName("createIndexes");
		conf.set("jobName", "createIndexes");
		if (!filteredAttrs.equals("")) {
			conf.setIfUnset("filteredAttrs", filteredAttrs);
		}

		String[] argsN = new String[args.length-2];
		for (int i = 2; i < args.length; i++) {
			argsN[i-2] = args[i];
		}

		// the keys are words (strings)
		conf.setOutputKeyClass(Text.class);
		// the values are counts (ints)
		conf.setOutputValueClass(Text.class);

		conf.setMapperClass(MapClass.class);
		conf.setReducerClass(Reduce.class);
		conf.setInputFormat(xInputFormat.class);
		conf.setNumReduceTasks(20);
		conf.setPartitionerClass(MyPartitioner.class);
		MultipleOutputs.addMultiNamedOutput(conf, "index", TextOutputFormat.class, Text.class, Text.class);

		List<String> other_args = new ArrayList<String>();
		for(int i=0; i < argsN.length; ++i) {
			try {
				if ("-m".equals(argsN[i])) {
					conf.setNumMapTasks(Integer.parseInt(argsN[++i]));
				} else if ("-r".equals(argsN[i])) {
					conf.setNumReduceTasks(Integer.parseInt(argsN[++i]));
				} else {
					other_args.add(argsN[i]);
				}
			} catch (NumberFormatException except) {
				System.out.println("ERROR: Integer expected instead of " + argsN[i]);
				return printUsage();
			} catch (ArrayIndexOutOfBoundsException except) {
				System.out.println("ERROR: Required parameter missing from " +
						argsN[i-1]);
				return printUsage();
			}
		}
		// Make sure there are exactly 2 parameters left.
		if (other_args.size() != 2) {
			System.out.println("ERROR: Wrong number of parameters: " +
					other_args.size() + " instead of 2.");
			return printUsage();
		}
		FileInputFormat.setInputPaths(conf, other_args.get(0));
		FileOutputFormat.setOutputPath(conf, new Path(other_args.get(1)));

		JobClient.runJob(conf);
		return 0;
	}


	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new CreateIndexesMy(), args);
		System.exit(res);
	}

}