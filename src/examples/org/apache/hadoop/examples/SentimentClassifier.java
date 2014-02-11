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

import java.io.File;

import com.aliasi.classify.ConditionalClassification;
import com.aliasi.classify.LMClassifier;
import com.aliasi.util.AbstractExternalizable;

public class SentimentClassifier { 
	String[] categories;
	LMClassifier classifier;

	public SentimentClassifier() {
		try { 
			classifier = (LMClassifier) AbstractExternalizable.readObject(new File("/home/mgferreira/classifier.txt"));
			categories = classifier.categories();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public String classify(String text) {
		ConditionalClassification classification = classifier.classify(text);
		return classification.bestCategory();
	} 
}
