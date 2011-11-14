package org.cloudcmp.tasks.store;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.cloudcmp.Adaptor;
import org.cloudcmp.Task;

public class PopulateQueueTask extends StoreTask {
	private static Map<Long, Thread> runningThreads = new HashMap<Long, Thread>();
	private static Map<Long, AsyncTask> runningAsyncTasks = new HashMap<Long, AsyncTask>();
	
	private int numMsgs;
	private String queueName;
	private int msgSize;

	public PopulateQueueTask(Adaptor adaptor) {
		super(adaptor);
		configs.put("queue_size", "100"); // the number of messages pre-inserted to the queue
		configs.put("msg_size", "1000"); // the size of each message (in Bytes)
	}
	
	public List<String> getConfigItems() {
		List<String> items = super.getConfigItems();
		items.add("queue_size");
		items.add("msg_size");
		return items;
	}
	
	public String getTaskName() {
		return "populatequeue";
	}
	
	public boolean requiresQueueStore() {
		return true;
	}

	public boolean isAsyncSupported() {
		return true;
	}
	
	public Map<String, String> run() {
		Map<String, String> results = new HashMap<String, String>();
		numMsgs = Integer.parseInt(configs.get("queue_size"));
		if (numMsgs < 0) {
			results.put("exception", "wrong num_msgs value");
			return results;
		}
		
		msgSize = Integer.parseInt(configs.get("msg_size"));
		if (msgSize <= 0) {
			results.put("exception", "wrong msg_size value");
			return results;
		}
		
		queueName = "queue" + String.valueOf(numMsgs);
		try {
			adaptor.queueDelete(queueName);
		}
		catch (Exception e) {}
		
		try {
			adaptor.queueCreate(queueName);
			Random rng = new Random();
			for (int i = 0; i < numMsgs; ++ i) {
				StringBuilder msgBuilder = new StringBuilder();
				for (int j = 0; j < msgSize; ++ j) {
					char c = (char)(rng.nextInt(26) + 65);
	                msgBuilder.append(c);
				}
				adaptor.queuePutMessage(queueName, msgBuilder.toString());
			}
			results.put("status", "ok");
			return results;
		}
		catch (IOException e) {
			results.put("exception", "IOException: " + e.getMessage());
			return results;
		}
	}
	
	public static void main(String [] args) {
		if (args.length < 4) {
			System.err.println("Arguments: [adaptor_name] [config_file] [msg_size] [num_msgs]");
			System.exit(1);
		}
		Adaptor adaptor = Adaptor.getAdaptorByName(args[0]);
		if (adaptor == null) {
			System.err.println("Unknown adaptor");
			System.exit(1);
		}
		
		adaptor.loadConfigFromFile(args[1]);		
		PopulateQueueTask task = new PopulateQueueTask(adaptor);
		task.configs.put("msg_size", args[2]);
		task.configs.put("num_msgs", args[3]);
		Map<String, String> results = task.run();
		Task.printResults(results);
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
			results = PopulateQueueTask.this.run();
		}
	}
}
