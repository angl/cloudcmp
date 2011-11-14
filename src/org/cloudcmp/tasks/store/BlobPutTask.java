package org.cloudcmp.tasks.store;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.cloudcmp.Adaptor;

public class BlobPutTask extends StoreTask {
	private int blobSize;
	private String containerName;
	private int concurrency;
	private boolean keep;
	byte [] contents;

	public BlobPutTask(Adaptor adaptor) {
		super(adaptor);
		configs.put("blob_size", "1000");
		configs.put("concurrency", "1");
		configs.put("keep", "false");
		// TODO Auto-generated constructor stub
	}

	public List<String> getConfigItems() {
		List<String> items = super.getConfigItems();
		items.add("blob_size");
		items.add("concurrency");
		items.add("keep");
		return items;
	}
	
	public String getTaskName() {
		return "blobput";
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
		
		keep = Boolean.parseBoolean(configs.get("keep"));
		
		Random rng = new Random();
		contents = new byte[blobSize];
		for (int i = 0; i < blobSize; ++ i)
            contents[i] = (byte)(rng.nextInt(256));		
		
		containerName = "container" + String.valueOf(blobSize);
		if (concurrency == 1 || !adaptor.supportMultipleThreads()) {
			String blobName = "temp-" + String.valueOf(blobSize) + "-" + String.valueOf(0);		
			try {
				long startTime = System.nanoTime();
				adaptor.blobUpload(containerName, blobName, contents);			
				long endTime = System.nanoTime();
				results.put("time", String.valueOf((endTime - startTime) / 1000));
				results.put("cost", String.valueOf(adaptor.getLastOperationCost()));
				
				if (!keep) adaptor.blobDelete(containerName, blobName);
			}
			catch (IOException e) {
				results.put("exception", "IOException: " + e.getMessage());
			}
			return results;
		}
		else {
			List<Thread> threads = new ArrayList<Thread>();
			List<UploadThread> uploads = new ArrayList<UploadThread>();
			List<String> blobNames = new ArrayList<String>();
			for (int i = 0; i < concurrency; ++ i) {
				String blobName = "temp-" + String.valueOf(blobSize) + "-" + String.valueOf(i);
				blobNames.add(blobName);
				UploadThread upload = new UploadThread(blobName);
				uploads.add(upload);
				Thread thread = new Thread(upload);
				threads.add(thread);
				thread.start();
			}
			
			for (int i = 0; i < concurrency; ++ i) {
				Thread thread = threads.get(i);
				UploadThread upload = uploads.get(i);
				try {
					thread.join();
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				long time = upload.runningTime;
				results.put("time_" + String.valueOf(i), String.valueOf(time / 1000));
			}
			
			if (!keep) {
				for (String blobName: blobNames) {
					try {
						adaptor.blobDelete(containerName, blobName);
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
			}
			return results;
		}
	}
	
	private class UploadThread implements Runnable {
		private String blobName;
		public long runningTime;
		
		public UploadThread(String blobName) {
			this.blobName = blobName;
		}
		
		@Override
		public void run() {
			try {
				long startTime = System.nanoTime();
				adaptor.blobUpload(containerName, blobName, contents);
				runningTime = System.nanoTime() - startTime;
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}			
		}
	}

}
