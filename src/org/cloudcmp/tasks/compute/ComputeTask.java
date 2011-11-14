package org.cloudcmp.tasks.compute;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.cloudcmp.Adaptor;
import org.cloudcmp.Task;

/**
 * The base class for all compute tasks
 * @author Ang Li
 *
 */
public class ComputeTask extends Task {
	protected static int finishedTasks = 0;
	protected static List<Long> runningTimes = new ArrayList<Long>();
	protected static Map<Long, Thread> runningThreads = new HashMap<Long, Thread>();
	protected static Map<Long, AsyncTask> runningAsyncTasks = new HashMap<Long, AsyncTask>();
	private int numThreads;
	private int numRuns;
	
	public ComputeTask(Adaptor adaptor) {
		super(adaptor);
	}

	public List<String> getConfigItems() {
		List<String> items = super.getConfigItems();
		items.add("num_threads");
		items.add("num_runs");
		return items;
	}
	
	public boolean isAsyncSupported() {
		return true;
	}
	
	public Map<String, String> run() {		
		Map<String, String> results = new HashMap<String, String>();
		if (!configs.containsKey("num_threads")) {
			numThreads = 1;
		}
		else if (configs.get("num_threads").equals("auto")) { // decide the num of threads by the num of CPU cores
			numThreads = Runtime.getRuntime().availableProcessors();
		}
		else numThreads = Integer.parseInt(configs.get("num_threads"));
		
		if (numThreads <= 0) {
			results.put("exception", "wrong num_threads value");
			return results;
		}
		
		if (!configs.containsKey("num_runs")) {
			numRuns = 1;
		}
		else numRuns = Integer.parseInt(configs.get("num_runs"));
		
		if (numRuns <= 0) {
			results.put("exception", "wrong num_runs value");
			return results;
		}
		
		if (!adaptor.supportMultipleThreads()) { // if multithreading is not supported, always fall back to the single-thread case
			SingleComputeTask task = getSingleComputeTask();
			finishedTasks = 0;
			runningTimes.clear();
			double startCost = adaptor.getComputeCost();
			task.run();
			double endCost = adaptor.getComputeCost();
			int i = 0;
			for (long runningTime:runningTimes) {
				results.put("time_" + String.valueOf(i), String.valueOf(runningTime));
				i ++;
			}
			double perRunCost = (endCost - startCost) / numRuns;
			results.put("cost", String.valueOf(perRunCost));
		}
		else {		
			List<Thread> threads = new ArrayList<Thread>();
			finishedTasks = 0;
			runningTimes.clear();
			double startCost = adaptor.getComputeCost();
			for (int i = 0; i < numThreads; ++ i) {
				SingleComputeTask singleTask = getSingleComputeTask();
				Thread thread = new Thread(singleTask);
				thread.start();
				threads.add(thread);	
			}
			for (Thread thread:threads) {
				try {
					thread.join();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
			double endCost = adaptor.getComputeCost();			
			int i = 0;
			for (long runningTime:runningTimes) {
				results.put("time_" + String.valueOf(i), String.valueOf(runningTime));
				i ++;
			}
			double perRunCost = (endCost - startCost) / numRuns;
			results.put("cost", String.valueOf(perRunCost));
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
			results = ComputeTask.this.run();
		}
	}
	
	protected SingleComputeTask getSingleComputeTask() {
		return new SingleComputeTask();
	}
	
	protected class SingleComputeTask implements Runnable {
		public void realRun() {
			// to be implemented by each individual task	
		}		
		
		@Override
		public void run() {
			while (true) {
				synchronized(ComputeTask.this) {					
					if (finishedTasks == numRuns) break;
					finishedTasks ++;
				}
				long startTime = System.nanoTime();
				realRun();
				long runningTime = (System.nanoTime() - startTime) / 1000;
				synchronized(ComputeTask.this) {
					runningTimes.add(runningTime);
				}
			}
		}
	}
}
