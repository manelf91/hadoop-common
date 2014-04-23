<%@page import="org.apache.jasper.compiler.JspUtil"%>
<%@page import="java.util.concurrent.TimeUnit"%>
<%@page import="org.apache.jasper.tagplugins.jstl.ForEach"%>
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
<%@ page import="javax.servlet.*" import="javax.servlet.http.*"
	import="java.io.*" import="java.text.*" import="java.util.*"
	import="java.text.DecimalFormat"
	import="org.apache.hadoop.http.HtmlQuoting"
	import="org.apache.hadoop.mapred.*" import="org.apache.hadoop.util.*"
	import="java.text.SimpleDateFormat"
	import="org.codehaus.jackson.map.ObjectMapper"%>
<!DOCTYPE html>
<html>
<head>
<title>Hadoop Map/Reduce Administration</title>
<script type="text/javascript" src="/static/jquery.min.js"></script>
<link rel="stylesheet" type="text/css" href="/static/bootstrap.min.css">
<link rel="stylesheet" type="text/css"
	href="/static/bootstrap-theme.min.css">
<link rel="stylesheet" type="text/css" href="/static/hadoop.css">
<script type="text/javascript" src="/static/bootstrap.min.js"></script>
<script>
	$(function() {
		$(".mid2").popover({
			html : true
		});

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
	<%!static SimpleDateFormat dateFormat = new SimpleDateFormat(
			"d-MMM-yyyy HH:mm:ss");%>
	<%!private static final long serialVersionUID = 1L;%>
	<%!private static DecimalFormat percentFormat = new DecimalFormat("##0.00");%>

	<%!public static class ErrorResponse {

		private final long errorCode;
		private final String errorDescription;

		// Constructor
		ErrorResponse(long ec, String ed) {

			errorCode = ec;
			errorDescription = ed;
		}

		// Getters
		public long getErrorCode() {
			return errorCode;
		}

		public String getErrorDescription() {
			return errorDescription;
		}
	}%>

	<%
		/* Eventually, the HTML output should also be driven off of these *Response
		 * objects. 
		 * 
		 * Someday. 
		 */

		/* ------------ Response generation begins here ------------ */

		response.setContentType("text/html; charset=UTF-8");

		final JobTracker tracker = (JobTracker) application
				.getAttribute("job.tracker");
		String trackerName = StringUtils.simpleHostname(tracker
				.getJobTrackerMachine());
		String jobid = request.getParameter("jobid");
		out.println("<h1>Job Statistics</h1>");
		if (jobid == null) {
			out.println("<h2>Missing 'jobid'!</h2>");
			return;
		}

		Set<String> splitLocationsSet = new HashSet<String>();
		final JobID jobidObj = JobID.forName(jobid);
		final JobInProgress job = tracker.getJob(jobidObj);
		TaskReport[] reports = tracker.getMapTaskReports(jobidObj);
		if (reports.length == 0) {
			out.println("<b> No Tasks found for the selected Job</b>");
			return;
		} else {
			SimpleDateFormat dateFormat = new SimpleDateFormat(
					"d-MMM-yyyy HH:mm:ss");
			Map<String, List<MapTaskStatistics>> groupedReports = new HashMap<String, List<MapTaskStatistics>>();
			long minStartTime = 0;
			long maxEndTime = 0;
			try {
				for (TaskReport report : reports) {
					if (report.getTaskID() != null) {
						TaskInProgress tip = job.getTaskInProgress(report
								.getTaskID());
						if (tip != null) {

							tip.getMapInputSize();
							TaskStatus[] available = tip.getTaskStatuses();

							for (TaskStatus running : available) {
								if ((running.getRunState()
										.equals(TaskStatus.State.SUCCEEDED))
										|| running.getRunState().equals(
												TaskStatus.State.KILLED)) {

									MapTaskStatistics stat = new MapTaskStatistics();
									stat.setTaskId(report.getTaskID()
											.toString());
									//TaskStatus running = available[available.length - 1];
									//out.print("Task status is"+ running.getRunState().name());
									stat.setMapStatus(running
											.getRunState().name());
									stat.setMapStartTime(running
											.getStartTime());
									stat.setMapEndtime(running
											.getFinishTime());
									//stat.setMapStartTimeDisplayFormat(StringUtils.getFormattedTimeWithDiff(dateFormat, running.getStartTime(), 0));
									// stat.setMapFinishedTimeDisplayFormat(StringUtils.getFormattedTimeWithDiff(dateFormat, running.getFinishTime(), 0));
									long duration = running.getFinishTime()
											- running.getStartTime();
									stat.setDuration(duration);

									stat.setSplitFileCount(tip
											.getSplitLocations().length);
									String locationString = tip
											.getSplitNodes();
									splitLocationsSet.add(locationString);
									stat.setSplitLocationsString(locationString);
									stat.setInputSplitLocations(StringUtils
											.split(locationString));
									//   out.println("split name"+tip.getSpl);

									minStartTime = JSPUtil.getMinStartTime(
											minStartTime,
											running.getStartTime());
									maxEndTime = JSPUtil.getMaxEndTime(
											maxEndTime,
											running.getFinishTime());
									Counters.Group group = running
											.getCounters()
											.getGroup(
													"org.apache.hadoop.mapred.Task$Counter");

									//out.println("Map Output is"+group.getCounterForName("MAP_OUTPUT_RECORDS").getValue()+"CPU Time is "+group.getCounter("CPU_MILLISECONDS"));
									stat.setCPUTime(group
											.getCounterForName(
													"CPU_MILLISECONDS")
											.getValue());
									stat.setMapOutputRecordCount(group
											.getCounterForName(
													"MAP_OUTPUT_RECORDS")
											.getValue());
									//out.println("Map output "+stat.getMapOutputRecordCount());

									//	out.println("Task Status is"+ running.getRunState());
									TaskTrackerStatus taskTracker = tracker
											.getTaskTrackerStatus(running
													.getTaskTracker());

									//register this servlet on task tracker- heta kranna
									String[] values = new JSPUtil()
											.getSplitCount(taskTracker
													.getHost(), taskTracker
													.getHttpPort(), running
													.getTaskID().toString());

									try {
										stat.setSplitCountStatus(values[0]);
										if (values[0].equals("success")) {
											stat.setSplitFileCount(Integer
													.parseInt(values[1]));
										}
									} catch (Exception ex) {
										stat.setSplitCountStatus("error");
									}
									//out.println("The split count " +values[0]+" and "+ values[1]+" and from stat "+stat.getSplitFileCount());
									//out.println("Host is " + taskTracker.getHost());
									//+"-"+taskTracker.getHttpPort());
									String host = taskTracker.getHost();
									if (groupedReports.containsKey(host)) {
										List<MapTaskStatistics> nodeList = groupedReports
												.get(host);
										nodeList.add(stat);
										groupedReports.put(host, nodeList);

									} else {
										List<MapTaskStatistics> nodeList = new ArrayList<MapTaskStatistics>();
										nodeList.add(stat);
										groupedReports.put(host, nodeList);

									}
								}
							}
						}
					}
				}
			} catch (Exception ex) {
				StringWriter errors = new StringWriter();
				ex.printStackTrace(new PrintWriter(errors));
				out.println(errors.toString());
			}
			long difference = (maxEndTime - minStartTime);
			//assign colors for the split location set
			String colorCodes = "#C67171:#71C671:#8470FF:#FFE4B5:#FF4500:#EEEE00:#8E388E:#292421:#008B00:#33A1C9:#FF00FF:#FFE1FF:#9400D3:#0000FF:71C671"; //to be moved into a config file later
			String[] colorArray = colorCodes.split(":");
			Map<String, String> locationToColorMap = new HashMap<String, String>();
			int colorCount = 0;
			for (String location : splitLocationsSet) {
				if (colorCount >= colorArray.length - 1) {
					locationToColorMap.put(location, "#9043B9");
				} else {
					locationToColorMap
							.put(location, colorArray[colorCount]);
					colorCount++;
				}
			}
			out.println("<b>"
					+ String.format(
							"Total time taken for the job is %.2f seconds",
							(double) difference / (1000)) + "</b><br/>");
			out.println("Data format represented by a task is: map output records;split file size;split location");
			double slotSize = (double) difference / (10 * 1000);
			for (Map.Entry<String, List<MapTaskStatistics>> entry : groupedReports
					.entrySet()) {
				out.println("<h4><b>Host Name:" + entry.getKey()
						+ ";Succeeded Map Count:" +new JSPUtil().getRunningMapCount(entry.getValue())
						+ "</b></h4>");
				List<MapTaskStatistics> mapsForNode = entry.getValue();
				List<MapTaskStatistics> headTasks = new JSPUtil()
						.getHeadTaskList(mapsForNode);
				out.println("<div class=\"timeline\">");
				long taskDuration = 0;
				double width = 0;
				double left = 0;
				int count = 0;
				String color = "#9043B9";
				MapTaskStatistics statTask = null;
				for (MapTaskStatistics bar : headTasks) {
					statTask = bar;
					count = 0;
					out.print(" <div class=\"events\">");
					while (statTask != null) {
						StringBuilder popupContentBuilder = new StringBuilder();
						StringBuilder inputSplitLocations = new StringBuilder();
						for (String location : statTask
								.getInputSplitLocations()) {
							inputSplitLocations.append(location);
							inputSplitLocations.append("<br/>");
						}
						popupContentBuilder.append(String.format(
								"Time spent for map : %.1f seconds",
								((float) statTask.getDuration() / 1000)));
						popupContentBuilder.append(String.format(
								"<br/> Map output records :%,d",
								statTask.getMapOutputRecordCount()));
						if (bar.getSplitCountStatus().equals("success")) {
							popupContentBuilder.append(String.format(
									"<br/>Split file size :%d",
									statTask.getSplitFileCount()));
						} else {
							popupContentBuilder
									.append(String
											.format("<br/>cat't get count from logs"));
						}
						popupContentBuilder.append(String.format(
								"<br/> Split Locations : %s",
								inputSplitLocations.toString()));
                      if(statTask.getMapStatus().equals(TaskStatus.State.SUCCEEDED.name())){
						color = locationToColorMap.get(statTask.getSplitLocationsString());
                      }
                      else{
                    	  color="#000000";
                      }
						taskDuration = statTask.getMapEndtime()
								- statTask.getMapStartTime();
						width = (double) ((taskDuration * 100))
								/ difference;
						if (statTask.getPreviousTask() == null) {
							left = (double) (((statTask.getMapStartTime() - minStartTime) * 100))
									/ difference;
						} else {
							left = (double) (((statTask.getMapStartTime() - statTask
									.getPreviousTask().getMapEndtime()) * 100))
									/ difference;
						}
						out.print("<div class=\"mid2\" data-placement=\"top\" rel=\"popover\" data-content=\""
								+ popupContentBuilder.toString()
								+ "\" data-original-title=\""
								+ statTask.getTaskId()
								+ "\" style=\"width:"
								+ width
								+ "%;background-color:"
								+ color
								+ ";color:black"
								+ ";overflow: hidden;text-overflow: ellipsis;white-space: nowrap;"
								+ "font-weight:normal"
								+ ";border:2px solid black"
								+ ";margin-left:"
								+ left
								+ "%\">"
								+ statTask.getMapOutputRecordCount()
								+ ";"
								+ statTask.getSplitFileCount()
								+ ";"
								+ statTask.getSplitLocationsString()
								+ "</div>");
						statTask = statTask.getNextTask();
						count++;
					}
					out.print("</div>");
				}
			}
			out.print("<div class=\"events\">");
			out.print("<div class=\"first\" >"
					+ String.format("%.2f", slotSize) + "</div>");
			for (int i = 2; i <= 10; i++) {
				out.print("<div class=\"first\" >"
						+ String.format("%.2f", slotSize * i) + "</div>");
			}
			out.print("</div");
			out.print("</div>");

			String xmlContent = JSPUtil.getXmlContent(jobid,
					groupedReports, difference);

			out.print("<div class=\"row\" style=\"padding-left:20px\"><h4><a id=\"view\" class=\"btn\" href=\"#\">View details &raquo;</a><a class=\"btn\" id=\"export\" >Save XML &raquo;</a></h4> <div id=\"xmlDiv\" style=\"padding-left:30px;\" class=\"collapse\">"
					+ xmlContent + "</div></div>");
		}
	%>

	<%
		out.println(JSPUtil.printStaticStatistics());
	%>