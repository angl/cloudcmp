package org.cloudcmp.tasks.store;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.cloudcmp.Adaptor;
import org.cloudcmp.Task;

public class PopulateBlobTask extends StoreTask {
	private static Map<Long, Thread> runningThreads = new HashMap<Long, Thread>();
	private static Map<Long, AsyncTask> runningAsyncTasks = new HashMap<Long, AsyncTask>();
	
	private String containerName;
	private int blobSize;
	private int numBlobs;

	public PopulateBlobTask(Adaptor adaptor) {
		super(adaptor);
		configs.put("blob_size", "1000"); // the size of each blob (in Bytes) 
		configs.put("num_blobs", "32"); // the number of blobs created (for storage bandwidth test)
	}
	
	public List<String> getConfigItems() {
		List<String> items = super.getConfigItems();
		items.add("blob_size");
		items.add("num_blobs");
		return items;
	}
	
	public String getTaskName() {
		return "populateblob";
	}
	
	public boolean requiresBlobStore() {
		return true;
	}

	public boolean isAsyncSupported() {
		return true;
	}
	
	public Map<String, String> run() {
		Map<String, String> results = new HashMap<String, String>();
		blobSize = Integer.parseInt(configs.get("blob_size"));
		if (blobSize <= 0) {
			results.put("exception", "wrong blob_size value");
			return results;
		}
		
		numBlobs = Integer.parseInt(configs.get("num_blobs"));
		if (numBlobs < 0) {
			results.put("exception", "wrong num_blobs value");
		}
		
		containerName = "container" + String.valueOf(blobSize);
		
		try {
			adaptor.blobDeleteContainer(containerName);
		}
		catch (Exception e) {}
		
		try {
			adaptor.blobCreateContainer(containerName);
			for (int i = 0; i < numBlobs; ++ i) {
				String blobName = "test-" + String.valueOf(blobSize) + "-" + String.valueOf(i);
				byte [] contents = new byte[blobSize];
				Random rng = new Random();
				for (int j = 0; j < blobSize; ++ j) {
					contents[j] = (byte)(rng.nextInt(256));
				}				
				adaptor.blobUpload(containerName, blobName, contents);
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
			System.err.println("Arguments: [adaptor_name] [config_file] [blob_size] [num_blobs]");
			System.exit(1);
		}
		Adaptor adaptor = Adaptor.getAdaptorByName(args[0]);
		if (adaptor == null) {
			System.err.println("Unknown adaptor");
			System.exit(1);
		}
		
		adaptor.loadConfigFromFile(args[1]);		
		PopulateBlobTask task = new PopulateBlobTask(adaptor);
		task.configs.put("blob_size", args[2]);
		task.configs.put("num_blobs", args[3]);
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
			results = PopulateBlobTask.this.run();
		}
	}
}
