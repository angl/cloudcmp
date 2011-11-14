package org.cloudcmp.tasks.store;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.cloudcmp.Adaptor;

public class QueueGetTask extends StoreTask {
	private int queueSize;
	private String queueName;

	public QueueGetTask(Adaptor adaptor) {
		super(adaptor);
		configs.put("queue_size", "100");
		// TODO Auto-generated constructor stub
	}

	public List<String> getConfigItems() {
		List<String> items = super.getConfigItems();
		items.add("queue_size");
		return items;
	}
	
	public String getTaskName() {
		return "queueget";
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
		
		queueName = "queue" + String.valueOf(queueSize);		
		try {
			long startTime = System.nanoTime();
			adaptor.queueGetMessage(queueName);
			long endTime = System.nanoTime();
			results.put("time", String.valueOf((endTime - startTime) / 1000));
			results.put("cost", String.valueOf(adaptor.getLastOperationCost()));
			adaptor.queueDeleteLastMessage(queueName);
		}
		catch (IOException e) {
			results.put("exception", "IOException: " + e.getMessage());
		}
		return results;
	}
}
