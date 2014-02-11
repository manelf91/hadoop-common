package org.apache.hadoop.mapred;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.StringReader;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SecureIOUtils;
import org.apache.hadoop.mapred.QueueManager.QueueACL;
import org.apache.hadoop.mapred.TaskLog.LogFileDetail;
import org.apache.hadoop.mapred.TaskLog.LogName;
import org.apache.hadoop.mapred.TaskLog.Reader;
import org.apache.hadoop.mapreduce.JobACL;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.AccessControlList;

public class JSPHelperServlet  extends HttpServlet {

	  public void doGet(HttpServletRequest request, 
              HttpServletResponse response
              ) throws ServletException, IOException {
		  

		    boolean isCleanup = false;
		    OutputStream out = response.getOutputStream();
		    String attemptIdStr = request.getParameter("attemptid");
		    if (attemptIdStr == null) {
		     out.write(("0;No attempt id").getBytes());
		      return;
		    }
		    TaskAttemptID attemptId = TaskAttemptID.forName(attemptIdStr);
		    if (!TaskLog.getAttemptDir(attemptId, isCleanup).exists()) {
		        out.write(("0;Task log dir not exist in order to get the count").getBytes());
		        return;
		      }
		    
		    String user = request.getRemoteUser();
		    if (user != null) {
		      ServletContext context = getServletContext();
		      TaskTracker taskTracker = (TaskTracker) context.getAttribute(
		          "task.tracker");
		      JobID jobId = attemptId.getJobID();

		      // get jobACLConf from ACLs file
		      JobConf jobACLConf = getConfFromJobACLsFile(jobId);
		      // Ignore authorization if job-acls.xml is not found
		      if (jobACLConf != null) {
		        try {
		          checkAccessForTaskLogs(jobACLConf, user,
		              jobId.toString(), taskTracker);
		        } catch (AccessControlException e) {
		         out.write(("0:No permission to access log").getBytes());
		          return;
		        }
		      }
		    }
		   getSplitFileCount(response, out, attemptId);
		 
	  }
	  private JobConf getConfFromJobACLsFile(JobID jobId) {
		    Path jobAclsFilePath = new Path(
		        TaskLog.getJobDir(jobId).toString(),
		        TaskTracker.jobACLsFile);
		    JobConf conf = null;
		    if (new File(jobAclsFilePath.toUri().getPath()).exists()) {
		      conf = new JobConf(false);
		      conf.addResource(jobAclsFilePath);
		    }
		    return conf;
		  }
	  
	  private void checkAccessForTaskLogs(JobConf conf, String user, String jobId,
		      TaskTracker tracker) throws AccessControlException {

		    if (!tracker.areACLsEnabled()) {
		      return;
		    }

		    // build job view ACL by reading from conf
		    AccessControlList jobViewACL = tracker.getJobACLsManager().
		        constructJobACLs(conf).get(JobACL.VIEW_JOB);

		    // read job queue name from conf
		    String queue = conf.getQueueName();

		    // build queue admins ACL by reading from conf
		    AccessControlList queueAdminsACL = new AccessControlList(
		        conf.get(QueueManager.toFullPropertyName(queue,
		            QueueACL.ADMINISTER_JOBS.getAclName()), " "));

		    String jobOwner = conf.get("user.name");
		    UserGroupInformation callerUGI =
		        UserGroupInformation.createRemoteUser(user);

		    // check if user is queue admin or cluster admin or jobOwner or member of
		    // job-view-acl
		    if (!queueAdminsACL.isUserAllowed(callerUGI)) {
		      tracker.getACLsManager().checkAccess(jobId, callerUGI, queue,
		          Operation.VIEW_TASK_LOGS, jobOwner, jobViewACL);
		    }
		  }

	  private void getSplitFileCount(HttpServletResponse response,
			OutputStream out, TaskAttemptID attemptId) throws IOException {
		// TODO Auto-generated method stub
		  try {
		      InputStream taskLogReader = new TaskLog.Reader(attemptId, TaskLog.LogName.SYSLOG, 0, -1, false);
		      String line;
		      BufferedReader reader = new BufferedReader(new InputStreamReader(taskLogReader));
		      while ((line = reader.readLine()) != null) {
		    	  if(line.contains("Processing split:"))
		    	  {
		      		break;
		    	  }
		      }
		      Pattern p2 = Pattern.compile("hdfs://", Pattern.CASE_INSENSITIVE);
		      Matcher m = p2.matcher(line);
		      int count = 0;
		      while (m.find())
		          count++;
		      
		      out.write(("success:"+count).getBytes());
	}
		  catch(Exception ex){
		      out.write(("error:"+0).getBytes());
		  }
	  }

}

