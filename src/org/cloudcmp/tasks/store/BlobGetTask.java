package org.cloudcmp.tasks.store;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.cloudcmp.Adaptor;

public class BlobGetTask extends StoreTask {
	private int blobSize;
	private String containerName;
	private int concurrency;

	public BlobGetTask(Adaptor adaptor) {
		super(adaptor);
		configs.put("blob_size", "1000");
		configs.put("concurrency", "1");
		// TODO Auto-generated constructor stub
	}

	public List<String> getConfigItems() {
		List<String> items = super.getConfigItems();
		items.add("blob_size");
		items.add("concurrency");
		return items;
	}
	
	public String getTaskName() {
		return "blobget";
	}
	
	public boolean requiresBlobStore() {
		return true;
	}
	
	public Map<String, String> run() {
		Map<String, String> results = new HashMap<String, String>();
		blobSize = Integer.parseInt(configs.get("blob_size"));
		if (blobSize <= 0) {
			results.put("exception", "wrong table_size value");
			return results;
		}
		
		concurrency = Integer.parseInt(configs.get("concurrency"));
		if (concurrency <= 0) {
			results.put("exception", "wrong concurrency value");
			return results;
		}		
		
		containerName = "container" + String.valueOf(blobSize);
		if (concurrency == 1 || !adaptor.supportMultipleThreads()) {
			String blobName = "test-" + String.valueOf(blobSize) + "-" + String.valueOf(0);		
			try {
				long startTime = System.nanoTime();
				adaptor.blobDownload(containerName, blobName, true);				
				long endTime = System.nanoTime();
				results.put("time", String.valueOf((endTime - startTime) / 1000));
				results.put("cost", String.valueOf(adaptor.getLastOperationCost()));
			}
			catch (IOException e) {
				results.put("exception", "IOException: " + e.getMessage());
			}
			return results;
		}
		else {
			List<Thread> threads = new ArrayList<Thread>();
			List<DownloadThread> downloads = new ArrayList<DownloadThread>();
			for (int i = 0; i < concurrency; ++ i) {
				String blobName = "test-" + String.valueOf(blobSize) + "-" + String.valueOf(i);
				DownloadThread download = new DownloadThread(blobName);
				downloads.add(download);
				Thread thread = new Thread(download);
				threads.add(thread);
				thread.start();
			}
			
			for (int i = 0; i < concurrency; ++ i) {
				Thread thread = threads.get(i);
				DownloadThread download = downloads.get(i);
				try {
					thread.join();
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				long time = download.runningTime;
				results.put("time_" + String.valueOf(i), String.valueOf(time / 1000));
			}
			return results;
		}
	}
	
	private class DownloadThread implements Runnable {
		private String blobName;
		public long runningTime;
		
		public DownloadThread(String blobName) {
			this.blobName = blobName;
		}
		
		@Override
		public void run() {
			try {
				long startTime = System.nanoTime();
				adaptor.blobDownload(containerName, blobName, true);
				runningTime = System.nanoTime() - startTime;
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}			
		}
	}

}
