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

package org.apache.hadoop.util;

import java.util.ArrayList;

public class StatisticsAgregator {
	private static StatisticsAgregator instance = new StatisticsAgregator();
	ArrayList<Object> registeredObjs = new ArrayList<Object>();
	public static StatisticsAgregator getInstance(){
		return instance;
	}
	private StatisticsAgregator(){
		
	}
	
	public void register(Object obj){
		this.registeredObjs.add(obj);
	}
	public ArrayList<Object> getRegisteredObjs(){
		return registeredObjs;
	}
}
