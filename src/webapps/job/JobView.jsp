<%!
/**
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
 */
%>
<%@ page
  import="javax.servlet.*"
  import="javax.servlet.http.*"
  import="java.io.*"
  import="java.text.*"
  import="java.util.*"
  import="java.text.DecimalFormat"
  import="org.apache.hadoop.http.HtmlQuoting"
  import="org.apache.hadoop.mapred.*"
  import="org.apache.hadoop.mapreduce.*"
  import="org.apache.hadoop.util.*"
  import="org.codehaus.jackson.map.ObjectMapper"
%>
<!DOCTYPE html>
<html>
<head>
<title>Hadoop Map/Reduce Administration</title>
<link rel="stylesheet" type="text/css" href="/static/bootstrap.min.css">
<link rel="stylesheet" type="text/css" href="/static/bootstrap-theme.min.css">
<link rel="stylesheet" type="text/css" href="/static/hadoop.css">
<script type="text/javascript" src="/static/bootstrap.min.js"></script>
</head>
<body>
<%!static SimpleDateFormat dateFormat = new SimpleDateFormat(
      "d-MMM-yyyy HH:mm:ss");
%>
<%!	private static final long serialVersionUID = 1L;
%>
<%!
  private static DecimalFormat percentFormat = new DecimalFormat("##0.00");
  
%>

<%! 
  public static class ErrorResponse {

    private final long errorCode;
    private final String errorDescription;

    // Constructor
    ErrorResponse(long ec, String ed) {

      errorCode = ec;
      errorDescription = ed;
    }

    // Getters
    public long getErrorCode() { return errorCode; }
    public String getErrorDescription() { return errorDescription; }
  }
%> 

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

  JobTracker tracker = (JobTracker) application.getAttribute("job.tracker");
  ClusterStatus status = tracker.getClusterStatus();
  ClusterMetrics metrics = tracker.getClusterMetrics();
  String trackerName = 
           StringUtils.simpleHostname(tracker.getJobTrackerMachine());
  JobQueueInfo[] queues = tracker.getQueues();
  Vector<JobInProgress> allJobs = tracker.allJobs();
  Collections.sort(allJobs, new Comparator<JobInProgress>() {
	    public int compare(JobInProgress one, JobInProgress other) {
	        return Long.valueOf(other.getStartTime()).compareTo(Long.valueOf(one.getStartTime()));
	    }
	}); 
%>

<%
if (allJobs.size() > 0) {
	out.print(JSPUtil.generateJobDropDown(allJobs,tracker.conf));

}
%>



<h2 id="local_logs">Local Logs</h2>
<a href="logs/">Log</a> directory,
<a href="<%=JobHistoryServer.getHistoryUrlPrefix(tracker.conf)%>/jobhistoryhome.jsp">
Job Tracker History</a>

<%
out.println(ServletUtil.htmlFooter());
%>

