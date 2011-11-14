package org.cloudcmp.tasks.store;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.cloudcmp.Adaptor;

public class QueuePutTask extends StoreTask {
	private int queueSize;
	private int msgSize;
	private String queueName;

	public QueuePutTask(Adaptor adaptor) {
		super(adaptor);
		configs.put("queue_size", "100");
		configs.put("msg_size", "1000");
		// TODO Auto-generated constructor stub
	}

	public List<String> getConfigItems() {
		List<String> items = super.getConfigItems();
		items.add("queue_size");
		items.add("msg_size");
		return items;
	}
	
	public String getTaskName() {
		return "queueput";
	}
	
	public boolean requiresQueueStore() {
		return true;
	}
	
	public Map<String, String> run() {
		Map<String, String> results = new HashMap<String, String>();
		queueSize = Integer.parseInt(configs.get("queue_size"));
		if (queueSize <= 0) {
			results.put("exception", "wrong queue_size value");
			return results;
		}
		
		msgSize = Integer.parseInt(configs.get("msg_size"));
		if (msgSize <= 0) {
			results.put("exception", "wrong msg_size value");
			return results;
		}
		
		queueName = "queue" + String.valueOf(queueSize);
		Random rng = new Random();
		StringBuilder msgBuilder = new StringBuilder();
		for (int j = 0; j < msgSize; ++ j) {
			char c = (char)(rng.nextInt(26) + 65);
            msgBuilder.append(c);
		}
		
		try {
			long startTime = System.nanoTime();
			adaptor.queuePutMessage(queueName, msgBuilder.toString());
			long endTime = System.nanoTime();
			results.put("time", String.valueOf((endTime - startTime) / 1000));
			results.put("cost", String.valueOf(adaptor.getLastOperationCost()));
		}
		catch (IOException e) {
			results.put("exception", "IOException: " + e.getMessage());
		}
		return results;
	}


}
