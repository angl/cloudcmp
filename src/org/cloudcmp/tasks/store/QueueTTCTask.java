package org.cloudcmp.tasks.store;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.cloudcmp.Adaptor;

public class QueueTTCTask extends StoreTask {
	private int queueSize;
	private int msgSize;
	private String queueName;

	public QueueTTCTask(Adaptor adaptor) {
		super(adaptor);
		configs.put("msg_size", "1000");
		// TODO Auto-generated constructor stub
	}

	public List<String> getConfigItems() {
		List<String> items = super.getConfigItems();
		items.add("msg_size");
		return items;
	}
	
	public String getTaskName() {
		return "queuettc";
	}
	
	public boolean requiresQueueStore() {
		return true;
	}
	
	public Map<String, String> run() {
		Map<String, String> results = new HashMap<String, String>();		
		msgSize = Integer.parseInt(configs.get("msg_size"));
		if (msgSize <= 0) {
			results.put("exception", "wrong msg_size value");
			return results;
		}
		
		queueName = "queue" + String.valueOf(0);
		Random rng = new Random();
		StringBuilder msgBuilder = new StringBuilder();
		for (int j = 0; j < msgSize; ++ j) {
			char c = (char)(rng.nextInt(26) + 65);
            msgBuilder.append(c);
		}
		
		try {
			adaptor.queueClear(queueName);			
			adaptor.queuePutMessage(queueName, msgBuilder.toString());
			long startTime = System.nanoTime();
			boolean found = false;
			boolean inconsistent = false;
			long endTime = 0;
			while (!found) {
				String msg = adaptor.queueGetMessage(queueName);
				if (msg != null) {
					found = true;
					endTime = System.nanoTime();
				}
				else {
					inconsistent = true;
				}
			}
			
			results.put("inconsistent", String.valueOf(inconsistent));
			results.put("ttc", String.valueOf((endTime - startTime) / 1000));
			adaptor.queueDeleteLastMessage(queueName);
		}
		catch (IOException e) {
			results.put("exception", "IOException: " + e.getMessage());
		}
		return results;
	}
}
