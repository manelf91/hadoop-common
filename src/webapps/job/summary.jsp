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
$(function ()
{ $(".mid2").popover({html:true});
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

		/* For now, "json" is the only supported format. 
		 *
		 * As more formats are supported, this should become a cascading 
		 * if-elsif-else block.
		 */

		// Spit out HTML only in the absence of the "format" query parameter.
		response.setContentType("text/html; charset=UTF-8");

		final JobTracker tracker = (JobTracker) application
				.getAttribute("job.tracker");
		String trackerName = StringUtils.simpleHostname(tracker
				.getJobTrackerMachine());
		String jobid = request.getParameter("jobid");

		if (jobid == null) {
			out.println("<h2>Missing 'jobid'!</h2>");
			return;
		}

		final JobID jobidObj = JobID.forName(jobid);
		final JobInProgress job = tracker.getJob(jobidObj);
		TaskReport[] reports = tracker.getMapTaskReports(jobidObj);
		if (reports.length == 0) {
			out.println("<b> No Tasks found for the selected Job</b>");
			return;
		} else {
			 SimpleDateFormat dateFormat = new SimpleDateFormat("d-MMM-yyyy HH:mm:ss");
			Map<String, List<MapTaskStatistics>> groupedReports = new HashMap<String, List<MapTaskStatistics>>();
			long minStartTime=0;long maxEndTime=0;
			try{
			for (TaskReport report : reports) 
			{	
				if (report.getTaskID() != null) {
					TaskInProgress tip = job.getTaskInProgress(report.getTaskID());
					if (tip != null) {
						
                        tip.getMapInputSize();
						TaskStatus[] available = tip.getTaskStatuses();
						MapTaskStatistics stat=new MapTaskStatistics();
						stat.setTaskId(report.getTaskID().toString());
						TaskStatus running = available[available.length - 1];
						stat.setMapStatus(running.getStateString());
						stat.setMapStartTime(running.getStartTime());
						stat.setMapEndtime(running.getFinishTime());
						//stat.setMapStartTimeDisplayFormat(StringUtils.getFormattedTimeWithDiff(dateFormat, running.getStartTime(), 0));
					   // stat.setMapFinishedTimeDisplayFormat(StringUtils.getFormattedTimeWithDiff(dateFormat, running.getFinishTime(), 0));
					    long duration=running.getFinishTime()-running.getStartTime();
					    stat.setDuration( duration);
	
					    stat.setSplitFileCount(tip.getSplitLocations().length);
					    stat.setInputSplitLocations(StringUtils.split(tip.getSplitNodes()));
					 //   out.println("split name"+tip.getSpl);
					

					    minStartTime=JSPUtil.getMinStartTime(minStartTime,running.getStartTime());
					    maxEndTime=JSPUtil.getMaxEndTime(maxEndTime,running.getFinishTime());
					    Counters.Group group = running.getCounters().getGroup("org.apache.hadoop.mapred.Task$Counter");
					 					    
					    //out.println("Map Output is"+group.getCounterForName("MAP_OUTPUT_RECORDS").getValue()+"CPU Time is "+group.getCounter("CPU_MILLISECONDS"));
					   stat.setCPUTime(group.getCounterForName("CPU_MILLISECONDS").getValue());
					   stat.setMapOutputRecordCount(group.getCounterForName("MAP_OUTPUT_RECORDS").getValue());
					    //out.println("Map output "+stat.getMapOutputRecordCount());
					 
					//	out.println("Task Status is"+ running.getRunState());
						TaskTrackerStatus taskTracker = tracker.getTaskTrackerStatus(running.getTaskTracker());
				
						//register this servlet on task tracker- heta kranna
						String[] values=new JSPUtil().getSplitCount(taskTracker.getHost(),taskTracker.getHttpPort(), running.getTaskID().toString());
								
						try{
							stat.setSplitCountStatus(values[0]);
							if(values[0].equals("success")){
						stat.setSplitFileCount(Integer.parseInt(values[1]));
							}
						}
						catch(Exception ex){
							stat.setSplitCountStatus("error");
						}
						//out.println("The split count " +values[0]+" and "+ values[1]+" and from stat "+stat.getSplitFileCount());
						//out.println("Host is " + taskTracker.getHost());
						//+"-"+taskTracker.getHttpPort());
						String host=taskTracker.getHost();
						if(groupedReports.containsKey(host)){
							List<MapTaskStatistics>nodeList=groupedReports.get(host);
							nodeList.add(stat);
							groupedReports.put(host, nodeList);
							
						}
						else
						{
							List<MapTaskStatistics>nodeList=new ArrayList<MapTaskStatistics>();
							nodeList.add(stat);
							groupedReports.put(host, nodeList);

						}
					}
				}

			}
			}
			catch(Exception ex){
				StringWriter errors = new StringWriter();
				ex.printStackTrace(new PrintWriter(errors));
                out.println(errors.toString()); 
			}
			long difference=(maxEndTime-minStartTime);
           out.println("<b>"+String.format("Total time taken for the job is %.2f seconds",(double)difference/(1000))+"</b><br/>");
		    for (Map.Entry<String, List<MapTaskStatistics>> entry : groupedReports.entrySet()) {
		    	out.println("<h4>Host Name:"+entry.getKey()+"</h4>");
		    	out.println("<b>There are "+entry.getValue().size()+" tasks running on this host</b><br/>");
		    	double slotSize=(double)difference/(10*1000);
		    	
		    	List<MapTaskStatistics>mapsForNode=entry.getValue();
		    	Collections.sort(mapsForNode, new Comparator<MapTaskStatistics>() {
		            @Override
		            public int compare(MapTaskStatistics  map1, MapTaskStatistics  map2)
		            {
		            	return Long.compare(map1.getMapStartTime(), map2.getMapStartTime());
		            }
		    	});
		    	List<MapTaskStatistics>headTasks=new JSPUtil().getHeadTaskList(mapsForNode);
		    	out.println("<div class=\"timeline\">");
		    	  long taskDuration = 0;
		             long width=0;
		             long left = 0;
		             int count = 0;
		             String color = "lightgreen";
		             MapTaskStatistics statTask=null;
		            for (MapTaskStatistics bar : headTasks)
		            {
		            	statTask = bar;
		                count = 0;
		     
		                out.print(" <div class=\"events\">");
		                while (statTask != null)
		                {
		                	   StringBuilder popupContentBuilder=new StringBuilder();
					        	 StringBuilder inputSplitLocations=new StringBuilder();
					        	 for(String location : statTask.getInputSplitLocations()){
					        		inputSplitLocations.append(location);
					        		inputSplitLocations.append("<br/>");
					        	 }
					        	 popupContentBuilder.append(String.format("Time spent for map : %.1f seconds",((float)statTask.getDuration()/1000 )));
					        	 popupContentBuilder.append(String.format("<br/> Map output records :%,d" , statTask.getMapOutputRecordCount()));
					        	 if(bar.getSplitCountStatus().equals("success")){
					        		 popupContentBuilder.append(String.format("<br/>Split file size :%d", statTask.getSplitFileCount())); 
					        	 }
					        	 else{
					        		 popupContentBuilder.append(String.format("<br/>cat't get count from logs")); 
					        	 }
					        	 popupContentBuilder.append(String.format("<br/> Split Locations : %s",inputSplitLocations.toString()));
				                
		                    color = (count % 2) == 0 ? "lightgreen" : "lightblue";
		                    taskDuration = statTask.getMapEndtime() - statTask.getMapStartTime();
		                    width = (taskDuration * 100) / difference;
		                    if (statTask.getPreviousTask() == null)
		                    {
		                        left = ((statTask.getMapStartTime() - minStartTime) * 100) / difference;
		                    }
		                    else
		                    {
		                        left = ((statTask.getMapStartTime() - statTask.getPreviousTask().getMapEndtime())*100) / difference; 
		                    }
		                    out.print("<div class=\"mid2\" data-placement=\"top\" rel=\"popover\" data-content=\""+ popupContentBuilder.toString()+"\" data-original-title=\""+statTask.getTaskId()+ "\" style=\"width:" + width + "%;background-color:" + color + ";margin-left:"+left + "%\">View</div>");
		                    statTask = statTask.getNextTask();
		                    count++;
		                }
		                out.print("</div>");
		            }
		            out.print("<div class=\"events\">");
		            out.print("<div class=\"first\" >"+String.format("%.2f", slotSize)+"</div>");
		            for (int i = 2; i <= 10; i++)
		            {
		                out.print("<div class=\"first\" >" + String.format("%.2f", slotSize*i) + "</div>");
		 
		            }
		                out.print("</div");
		            out.print("</div>");

		    	
		    	
		    	
		    	
		    	
		    	
		      /*   for (MapTaskStatistics stat : entry.getValue()) {
		        	 long taskStartime=stat.getMapStartTime();
		        	 long taskEndTime=stat.getMapEndtime();
		        	 long left=taskStartime>minStartTime?(taskStartime-minStartTime)*100/difference:0;
		        	 long width=stat.getDuration()*100/difference;
		        	 StringBuilder popupContentBuilder=new StringBuilder();
		        	 StringBuilder inputSplitLocations=new StringBuilder();
		        	 for(String location : stat.getInputSplitLocations()){
		        		inputSplitLocations.append(location);
		        		inputSplitLocations.append("<br/>");
		        	 }
		        	 popupContentBuilder.append(String.format("Time spent for map : %.1f seconds",((float)stat.getDuration()/1000 )));
		        	 popupContentBuilder.append(String.format("<br/> Map output records :%,d" , stat.getMapOutputRecordCount()));
		        	 if(stat.getSplitCountStatus().equals("success")){
		        		 popupContentBuilder.append(String.format("<br/>Split file size :%d", stat.getSplitFileCount())); 
		        	 }
		        	 else{
		        		 popupContentBuilder.append(String.format("<br/>cat't get count from logs")); 
		        	 }
		        	 popupContentBuilder.append(String.format("<br/> Split Locations : %s",inputSplitLocations.toString()));
		        	out.println("<li style=\"width:"+width+"%;left:"+ left+"%;\"><div class=\"details\" data-placement=\"top\" rel=\"popover\" data-content=\""+ popupContentBuilder.toString()+"\" data-original-title=\""+stat.getTaskId()+"\">View Details </div></li>") ;
		          //out.println("<li style=\"width:200px;left:25px\">Width is:"+width+"left is"+left+"</li>") ;
		        	//out.println(stat.getMapStartTime()+" "+stat.getMapEndtime());
		            //out.println("\nTime spend in milliesecs"+stat.getDuration());
		        }
		         out.println("</ul><ul class=\"intervals\">");
		         out.println("<li class=\"first\">"+String.format("%.2f", slotSize)+"</li>");
		         for(int i=2;i<=9;i++){
		        	 out.println("<li>"+String.format("%.2f", slotSize*i)+"</li>");
		         }
		         out.println("<li class=\"last\">"+String.format("%.2f", slotSize*10)+"</li>");
		         out.println("</ul></div><br/>");
		       //  out.println("<ul><li id=\"example\" class=\"btn btn-success\" rel=\"popover\" data-content=\"It's so simple to create a tooltop for my website!\" data-original-title=\"Twitter Bootstrap Popover\">hover for popover</li></ul>");
		        */
		    }
			
		}
		
	%>





	<h2 id="local_logs">Local Logs</h2>
	<a href="logs/">Log</a> directory,
	<a
		href="<%=JobHistoryServer.getHistoryUrlPrefix(tracker.conf)%>/jobhistoryhome.jsp">
		Job Tracker History</a>

	<%
		out.println(ServletUtil.htmlFooter());
	%>