<%!/**
	 *  Licensed under the Apache License, Version 2.0 (the "License");
	 *  you may not use this file except in compliance with the License.
	 *  You may obtain a copy of the License at
	 *
	 *      http://www.apache.org/licenses/LICENSE-2.0
	 *
	 *  Unless required by applicable law or agreed to in writing, software
	 *  distributed under the License is distributed on an "AS IS" BASIS,
	 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
	 *  See the License for the specific language governing permissions and
	 *  limitations under the License.
	 */%>
<%@ page contentType="text/html; charset=UTF-8" import="javax.servlet.*"
	import="javax.servlet.http.*" import="java.io.*" import="java.util.*"
	import="java.text.DecimalFormat"
	import="org.apache.hadoop.http.HtmlQuoting"
	import="org.apache.hadoop.mapred.*" import="org.apache.hadoop.util.*"%>
<%!private static final long serialVersionUID = 1L;%>
<%
	TaskTracker tracker = (TaskTracker) application.getAttribute("task.tracker");
  String trackerName = tracker.getName();
%>

<!DOCTYPE html>
<html>
<head>
<title><%=trackerName%> Task Tracker Status</title>
<script type="text/javascript" src="/static/jquery.min.js"></script>
<link rel="stylesheet" type="text/css" href="/static/bootstrap.min.css">
<link rel="stylesheet" type="text/css"
	href="/static/bootstrap-theme.min.css">
<link rel="stylesheet" type="text/css" href="/static/hadoop.css">
<script type="text/javascript" src="/static/bootstrap.min.js"></script>
<script>
	$(function() {

		$('#view').on('click', function(e) {
			e.preventDefault();
			$("#xmlDiv").collapse('toggle');
		});

		document.getElementById("export").download = "export.xml";
		var cont = document.getElementById("xmlDiv").innerHTML;
		cont = cont.replace(/&lt;/g, "<");
		cont = cont.replace(/&gt;/g, ">");
		cont = cont.replace(/<br>/g, "");
		document.getElementById("export").href = "data:text/xml;charset=utf-8,"
				+ cont; //document.getElementById("sample").innerHTML;
		// this.innerHTML = "[Export conent]";
	});
</script>
</head>
<body>
	<%
		out.println("<h1>" + trackerName + " Statistics</h1>");
		NewMapTaskStatistics mps = new NewMapTaskStatistics();
		StringBuilder xmlBuilder = new StringBuilder();
		xmlBuilder.append("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n");
		xmlBuilder.append("<Tracker name=\"" + trackerName + "\">\n");
		xmlBuilder.append("<Statistics>\n");
		out.print("<div style=\"padding-left:40px\">\n");
		out.print("<div style=\"padding-left:40px;width:400px;background-color:#00FFFF\">\nIndex Statistic Details\n</div>\n");
		out.print("<table class=\"table table-striped\" style=\"max-width:400px\">\n");
		java.lang.reflect.Method[] methods = mps.getClass().getMethods();
		for (java.lang.reflect.Method meth : methods) {
			Statistics annos = meth.getAnnotation(Statistics.class);
			if (annos != null) {
				try {
					String line = (String) meth.invoke(mps);
					String[] keyValPair = line.split(":");
					out.print("<tr>\n<td>\n" + keyValPair[0]
							+ "\n</td>\n<td>\n" + keyValPair[1]
							+ "\n</td>\n</tr>\n");
					keyValPair[0] = keyValPair[0].replaceAll("\\s+", "");
					xmlBuilder.append("<" + keyValPair[0] + ">"
							+ keyValPair[1] + "</" + keyValPair[0] + ">\n");
				} catch (Exception ex) {
					out.print(ex.getMessage());
				}
			}
		}
		out.print("</table>\n");
		xmlBuilder.append("</Statistics>\n");
		xmlBuilder.append("</Tracker>\n");
		String xmlString = xmlBuilder.toString().replace("<", "&lt;");
		xmlString = xmlString.replace(">", "&gt;");
		xmlString = xmlString.replace("\n", "<br/>");
		out.print("<div class=\"row\" style=\"padding-left:20px\"><h4><a id=\"view\" class=\"btn\" href=\"#\">View details &raquo;</a><a class=\"btn\" id=\"export\" >Save XML &raquo;</a></h4> <div id=\"xmlDiv\" style=\"padding-left:30px;\" class=\"collapse\">"
				+ xmlString + "</div></div>");
	%>