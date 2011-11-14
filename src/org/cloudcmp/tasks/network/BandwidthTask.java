package org.cloudcmp.tasks.network;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.cloudcmp.Adaptor;
import org.cloudcmp.Task;

public class BandwidthTask extends Task {
	protected static List<Long> runningTimes = new ArrayList<Long>();
	protected static Map<Long, Thread> runningThreads = new HashMap<Long, Thread>();
	protected static Map<Long, AsyncTask> runningAsyncTasks = new HashMap<Long, AsyncTask>();
	private String targetAddr;
	private long transferSize;

	public BandwidthTask(Adaptor adaptor) {
		super(adaptor);
		configs.put("target_addr", "127.0.0.1");
		configs.put("transfer_size", "128");
		// TODO Auto-generated constructor stub
	}
	
	public String getTaskName() {
		return "bandwidthtask";
	}
	
	public List<String> getConfigItems() {
		List<String> items = super.getConfigItems();
		items.add("target_addr");
		items.add("transfer_size");
		return items;
	}
	
	public boolean requiresNetwork() {
		return true;
	}
	
	public boolean isAsyncSupported() {
		return true;
	}
	
	public Map<String, String> run() {
		Map<String, String> results = new HashMap<String, String>();
		targetAddr = configs.get("target_addr");
		transferSize = Integer.parseInt(configs.get("transfer_size"));
		if (transferSize <= 0 || transferSize % 8 != 0) {
			results.put("exception", "invalid transfer_size configuration");
			return results;
		}
		
		try {
			long bw = adaptor.measureBandwidth(targetAddr, transferSize * 1000000);
			results.put("bandwidth", String.valueOf(bw));
		}
		catch (IOException e) {
			results.put("exception", "IOException: " + e.getMessage());
		}
		
		return results;
	}
	
	public long start() {
		AsyncTask task = new AsyncTask();
		Thread thread = new Thread(task);
		thread.start();
		synchronized(this) {
			runningThreads.put(new Long(thread.getId()), thread);
			runningAsyncTasks.put(new Long(thread.getId()), task);
		}		
		return thread.getId();
	}
	
	@SuppressWarnings("deprecation")
	public void stop(long handle) {
		Thread thread = null;
		synchronized(this) {
			if (runningThreads.containsKey(handle)) {
				thread = runningThreads.get(handle);
				runningThreads.remove(handle);
				runningAsyncTasks.remove(handle);
			}				
		}
		if (thread != null) thread.stop();
	}
	
	public boolean isFinished(long handle) {
		boolean finished = true;
		Thread thread = null;
		synchronized(this) {
			if (runningThreads.containsKey(handle)) thread = runningThreads.get(handle); 
		}
		
		if (thread != null) {
			if (thread.isAlive()) finished = false;
		}
		
		return finished;
	}
	
	public Map<String, String> retrieveResults(long handle) {
		Thread thread = null;
		AsyncTask task = null;
		synchronized(this) {
			if (runningThreads.containsKey(handle)) {
				thread = runningThreads.get(handle);
				task = runningAsyncTasks.get(handle);
				runningThreads.remove(handle);
				runningAsyncTasks.remove(handle);
			}
		}
		
		if (thread == null || thread.isAlive()) return null;	
		return task.getResults();
	}
	
	protected class AsyncTask implements Runnable {
		private Map<String, String> results;
		
		public Map<String, String> getResults() {
			return results;
		}
		
		@Override
		public void run() {
			results = BandwidthTask.this.run();
		}
	}

}
