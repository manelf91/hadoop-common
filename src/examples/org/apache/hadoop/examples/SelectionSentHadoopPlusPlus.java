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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
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
import org.apache.hadoop.mapred.xRecordReader;
import org.apache.hadoop.mapred.lib.xInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
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
public class SelectionSentHadoopPlusPlus extends Configured implements Tool {


	/**
	 * Counts the words in each line.
	 * For each line of input, break the line into words and emit them as
	 * (<b>word</b>, <b>1</b>).
	 */
	public static class MapClass extends MapReduceBase
	implements Mapper<LongWritable, Text, Text, Text> {				

		private static HashMap<Integer, String> filtersMap = new HashMap<Integer, String>();

		public void configure(JobConf job) {        
			String filters = job.get("filteredAttrs");
			if (filters != null) {

				String[] filtersByAttr = filters.split(",");

				for (String filteredAttr : filtersByAttr) {
					String body = job.get("filter" + filteredAttr);

					int attrNr = Integer.parseInt(body.substring(0, body.indexOf(" ")));
					String filter = body.substring(body.indexOf(" ")+1);

					filtersMap.put(new Integer(attrNr), filter);
				}
			}
		}

		public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
			String line = value.toString();

			long start = System.currentTimeMillis();

			ArrayList<String> listdata1 = new ArrayList<String>();
			JsonParser parser = new JsonParser();
			JsonArray jArray1 = parser.parse(line).getAsJsonArray();
			if (jArray1 != null) { 
				for (int i=0; i < jArray1.size(); i++){ 
					listdata1.add(jArray1.get(i).toString());
				}
			}
			String user = listdata1.get(22);
			String location = "";
			String language = "";
			try {
				String after1 = user.split(", location=")[1];
				location = after1.split(", name=")[0];
			} catch(Exception e) {
				String after1 = user.split("location=")[1];
				location = after1.split(", lang=")[0];
			}
			location = "location:" +  location;

			try {
				String after2 = user.split(", lang=")[1];
				language = after2.split(", listed_count=")[0];
			} catch(Exception e) {
				language = user.split(", lang=")[1];
			}
			language = "lang:" +  language;

			String text = listdata1.get(19);

			String filter = filtersMap.get(0);
			if(language.equals(filter)) {
				SentimentClassifier sentClassifier = new SentimentClassifier();
				String sent = sentClassifier.classify(text);
				output.collect(new Text(language), new Text(sent));
			} else {
				xRecordReader.relevantBlock = -1;
			}
			long end = System.currentTimeMillis();
			MapTask.increaseMapFunctionTime(end-start);
		}
	}

	/**
	 * A reducer class that just emits the sum of the input values.
	 */
	public static class Reduce extends MapReduceBase
	implements Reducer<Text, Text, Text, Text> {

		public void reduce(Text key, Iterator<Text> values,
				OutputCollector<Text, Text> output, 
				Reporter reporter) throws IOException {

			HashMap<String, Integer> sentiments = new HashMap<String, Integer>();

			while (values.hasNext()) {
				String sent = values.next().toString();
				Integer cnt = sentiments.get(sent);
				if (cnt == null) {
					cnt = new Integer(1);
					sentiments.put(sent, cnt);
				}
				else {
					sentiments.put(sent, new Integer(cnt.intValue()+1));
				}
			}
			output.collect(key, new Text(sentiments.toString()));
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
		JobConf conf = new JobConf(getConf(), SelectionSentHadoopPlusPlus.class);

		/*mgferreira*/
		String jobName = args[0];
		conf.setJobName(jobName);
		conf.set("jobName", jobName);

		String blocksPerSplit = args[1];
		if(blocksPerSplit.contains("-")) {
			conf.setBooleanIfUnset("equal.splits", false);
		}
		else {
			conf.setBooleanIfUnset("equal.splits", true);
		}
		conf.setIfUnset("blocks.per.split", blocksPerSplit);

		String localityFirst = args[2];
		if (localityFirst.equals("true")) {
			conf.setBooleanIfUnset("mapred.locality.or.biggest.tasks.first", true);
		}
		else {
			conf.setBooleanIfUnset("mapred.locality.or.biggest.tasks.first", false);
		}

		String relevantAttrs = "";
		String filteredAttrs = "";

		BufferedReader br = new BufferedReader(new FileReader(args[3]));
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

		/*for (String s : filtersANDrelevantAttrs) {
			System.out.println(s);
			String absAttr = "";
			if(s.contains("=")) {
				absAttr = s.split("=")[0];
				String relativeAttrANDfilter = s.split("=")[1];
				filters += relativeAttrANDfilter + ",";
				filtersForMapFunction += n + "-" + s.split("=")[1].split("-")[1] + ",";
			}
			else {
				absAttr = s;
			}
			relevantAttrs += absAttr + ":";
			n++;
		}*/
		conf.setIfUnset("relevantAttrs", relevantAttrs);
		if (!filteredAttrs.equals("")) {
			conf.setIfUnset("filteredAttrs", filteredAttrs);
		}

		conf.setIfUnset("useIndexes", args[4]);


		String[] argsN = new String[args.length-5];
		for (int i = 5; i < args.length; i++) {
			argsN[i-5] = args[i];
		}

		// the keys are words (strings)
		conf.setOutputKeyClass(Text.class);
		// the values are counts (ints)
		conf.setOutputValueClass(Text.class);

		conf.setMapperClass(MapClass.class);
		conf.setReducerClass(Reduce.class);
		conf.setInputFormat(xInputFormat.class);

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
		int res = ToolRunner.run(new Configuration(), new SelectionSentHadoopPlusPlus(), args);
		System.exit(res);
	}
}

