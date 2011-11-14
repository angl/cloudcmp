package org.cloudcmp.tasks.store;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.cloudcmp.Adaptor;

public class BlobTTCTask extends StoreTask {
	private int blobSize;
	private String containerName;
	byte [] contents;

	public BlobTTCTask(Adaptor adaptor) {
		super(adaptor);
		configs.put("blob_size", "1000");
		// TODO Auto-generated constructor stub
	}

	public List<String> getConfigItems() {
		List<String> items = super.getConfigItems();
		items.add("blob_size");
		return items;
	}
	
	public String getTaskName() {
		return "blobttc";
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
		
		Random rng = new Random();
		contents = new byte[blobSize];
		for (int i = 0; i < blobSize; ++ i)
            contents[i] = (byte)(rng.nextInt(256));		
		
		containerName = "container" + String.valueOf(blobSize);
		String blobName = "temp-" + String.valueOf(blobSize) + "-ttc";
		try {
			adaptor.blobDelete(containerName, blobName);
		}
		catch (Exception e) {}
		
		try {			
			adaptor.blobUpload(containerName, blobName, contents);
			long startTime = System.nanoTime();
			boolean found = false;
			boolean inconsistent = false;
			long endTime = 0;
			while (!found) {				
				byte [] blob = adaptor.blobDownload(containerName, blobName, true);
				if (blob != null) {
					found = true;
					endTime = System.nanoTime();
				}
				else {
					inconsistent = true;
				}
			}
			results.put("inconsistent", String.valueOf(inconsistent));
			results.put("ttc", String.valueOf((endTime - startTime) / 1000));				
			adaptor.blobDelete(containerName, blobName);
			return results;
		}
		catch (IOException e) {
			results.put("exception", "IOException: " + e.getMessage());
			return results;
		}
	}
}
