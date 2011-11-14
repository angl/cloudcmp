package org.cloudcmp.tasks.store;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.cloudcmp.Adaptor;
import org.cloudcmp.Task;
import org.cloudcmp.store.Row;

public class PopulateTableTask extends StoreTask {	
	private static Map<Long, Thread> runningThreads = new HashMap<Long, Thread>();
	private static Map<Long, AsyncTask> runningAsyncTasks = new HashMap<Long, AsyncTask>();
	
	public static final int DATA_LENGTH = 50;
	public static final int BATCH_SIZE = 25;
	public static final int MAX_THREADS = 10;
		
	private int tableSize;
	private String tableName;

	public PopulateTableTask(Adaptor adaptor) {
		super(adaptor);
		configs.put("table_size", "1000");
	}
	
	public List<String> getConfigItems() {
		List<String> items = super.getConfigItems();
		items.add("table_size");
		return items;
	}
	
	public String getTaskName() {
		return "populatetable";
	}
	
	public boolean requiresTableStore() {
		return true;
	}

	public boolean isAsyncSupported() {
		return true;
	}
	
	private Row makeRow(int id) {
		Row r = new Row(String.valueOf(id));
		String nonkey = String.valueOf((int)(Math.random() * tableSize / 10));
		StringBuilder randoms = new StringBuilder();
		for (int i = 0; i < DATA_LENGTH; ++ i) {
			randoms.append((char)((int)(Math.random() * 26) + 65));
		}
		r.setString("nonkey", nonkey);
		r.setString("data", randoms.toString());
		return r;
	}
	
	public Map<String, String> run() {
		Map<String, String> results = new HashMap<String, String>();
		tableSize = Integer.parseInt(configs.get("table_size"));
		if (tableSize < 0) {
			results.put("exception", "wrong table_size value");
			return results;
		}
		
		tableName = "table" + String.valueOf(tableSize);
		try {
			adaptor.tableDelete(tableName);
		}
		catch (Exception e) {}
		
		try {
			adaptor.tableCreate(tableName);
			if (tableSize <= BATCH_SIZE) { // add one by one
				for (int i = 0; i < tableSize; ++ i) {
					Row r = makeRow(i);
					adaptor.tableInsertRow(tableName, r);
				}
			}
			else {
				if (!adaptor.supportMultipleThreads()) { // if multithreading is not supported, we can only sequentially populate everything
					// send in batches
					for (int i = 0; i < tableSize / BATCH_SIZE; ++ i) {
						List<Row> rows = new ArrayList<Row>();
						for (int j = 0; j < BATCH_SIZE; ++ j) {
							rows.add(makeRow(i * BATCH_SIZE + j));
						}
						adaptor.tableInsertRows(tableName, rows);
					}
				}
				else { // support multithreading, create multiple populating threads
					int numThreads = MAX_THREADS;
					if (tableSize / BATCH_SIZE < MAX_THREADS) numThreads = tableSize / BATCH_SIZE;
					List<Thread> threads = new ArrayList<Thread>();
					for (int i = 0; i < numThreads; ++ i) {
						int numBatches = tableSize / numThreads / BATCH_SIZE;
						int fromID = i * numBatches * BATCH_SIZE;
						Thread thread = new Thread(new populateThread(numBatches, fromID));
						thread.start();
						threads.add(thread);
					}
					for (Thread thread: threads) {
						try {
							thread.join();
						} catch (InterruptedException e) {
							e.printStackTrace();
						}
					}
				}
			}
			results.put("status", "ok");
		} catch (IOException e) {
			results.put("exception", "IOException: " + e.getMessage());			
		}		

		return results;
	}
	
	public static void main(String [] args) {
		if (args.length < 3) {
			System.err.println("Arguments: [adaptor_name] [config_file] [table_size]");
			System.exit(1);
		}
		Adaptor adaptor = Adaptor.getAdaptorByName(args[0]);
		if (adaptor == null) {
			System.err.println("Unknown adaptor");
			System.exit(1);
		}
		
		adaptor.loadConfigFromFile(args[1]);		
		PopulateTableTask task = new PopulateTableTask(adaptor);
		task.configs.put("table_size", args[2]);
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
			results = PopulateTableTask.this.run();
		}
	}
	
	private class populateThread implements Runnable {
		private int fromID;
		private int numBatches;
		
		public populateThread(int numBatches, int fromID) {
			this.numBatches = numBatches;
			this.fromID = fromID;
		}
		
		@Override
		public void run() {
			for (int i = 0; i < numBatches; ++ i) {
				List<Row> rows = new ArrayList<Row>();
				for (int j = 0; j < BATCH_SIZE; ++ j) {
					rows.add(makeRow(fromID + i * BATCH_SIZE + j));
				}
				boolean retry = true;
				while (retry) {
					try {
						adaptor.tableInsertRows(tableName, rows);
						retry = false;
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
			}
		}
	}
}
