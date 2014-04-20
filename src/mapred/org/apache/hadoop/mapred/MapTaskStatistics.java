package org.apache.hadoop.mapred;

import java.util.Date;
import java.util.List;

import javax.print.attribute.standard.DateTimeAtCompleted;

public class MapTaskStatistics {

	private long Duration;
	private String divColorCode;
	private long MapStartTime;
	private long MapEndtime;
	private String MapStartTimeDisplayFormat;
	private String MapFinishedTimeDisplayFormat;
	private String MapStatus;;
	private long MapOutputRecordCount;
	private long CPUTime;
	private String TaskId;
	private int splitFileCount;
	private String splitCountStatus;
	private String[] inputSplitLocations;
	private long BlockSize;
	private MapTaskStatistics NextTask;
	private MapTaskStatistics previousTask;
	private String splitLocationsString;
	public long getDuration() {
		return Duration;
	}
	public void setDuration(long durationInSeconds) {
		Duration = durationInSeconds;
	}
	public String getMapStartTimeDisplayFormat() {
		return MapStartTimeDisplayFormat;
	}
	public void setMapStartTimeDisplayFormat(String mapStartTime) {
		MapStartTimeDisplayFormat = mapStartTime;
	}
	public String getMapFinishedTimeDisplayFormat() {
		return MapFinishedTimeDisplayFormat;
	}
	public void setMapFinishedTimeDisplayFormat(String mapFinishedTime) {
		MapFinishedTimeDisplayFormat = mapFinishedTime;
	}
	public String getMapStatus() {
		return MapStatus;
	}
	public void setMapStatus(String mapStatus) {
		MapStatus = mapStatus;
	}
	public long getMapOutputRecordCount() {
		return MapOutputRecordCount;
	}
	public void setMapOutputRecordCount(long mapOutputRecordCount) {
		MapOutputRecordCount = mapOutputRecordCount;
	}
	public long getCPUTime() {
		return CPUTime;
	}
	public void setCPUTime(long cPUTime) {
		CPUTime = cPUTime;
	}
	public long getBlockSize() {
		return BlockSize;
	}
	public void setBlockSize(long blockSize) {
		BlockSize = blockSize;
	}
	public long getMapStartTime() {
		return MapStartTime;
	}
	public void setMapStartTime(long mapStartTime) {
		MapStartTime = mapStartTime;
	}
	public long getMapEndtime() {
		return MapEndtime;
	}
	public void setMapEndtime(long mapEndtime) {
		MapEndtime = mapEndtime;
	}
	public String getTaskId() {
		return TaskId;
	}
	public void setTaskId(String taskId) {
		TaskId = taskId;
	}
	public int getSplitFileCount() {
		return splitFileCount;
	}
	public void setSplitFileCount(int splitFileCount) {
		this.splitFileCount = splitFileCount;
	}
	public String[] getInputSplitLocations() {
		return inputSplitLocations;
	}
	public void setInputSplitLocations(String[] inputSplitLocations) {
		this.inputSplitLocations = inputSplitLocations;
	}
	public String getSplitCountStatus() {
		return splitCountStatus;
	}
	public void setSplitCountStatus(String splitCountStatus) {
		this.splitCountStatus = splitCountStatus;
	}
	public MapTaskStatistics getNextTask() {
		return NextTask;
	}
	public void setNextTask(MapTaskStatistics nextTask) {
		NextTask = nextTask;
	}
	public MapTaskStatistics getPreviousTask() {
		return previousTask;
	}
	public void setPreviousTask(MapTaskStatistics previousTask) {
		this.previousTask = previousTask;
	}
	public String getDivColorCode() {
		return divColorCode;
	}
	public void setDivColorCode(String divColorCode) {
		this.divColorCode = divColorCode;
	}
	public String getSplitLocationsString() {
		return splitLocationsString;
	}
	public void setSplitLocationsString(String splitLocationsString) {
		this.splitLocationsString = splitLocationsString;
	}

}
