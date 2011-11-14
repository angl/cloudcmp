package org.cloudcmp.tasks.network;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.cloudcmp.Adaptor;
import org.cloudcmp.Task;

public class LatencyTask extends Task {
	private String targetAddr;

	public LatencyTask(Adaptor adaptor) {
		super(adaptor);
		configs.put("target_addr", "127.0.0.1");
		// TODO Auto-generated constructor stub
	}

	public String getTaskName() {
		return "latencytask";
	}
	
	public List<String> getConfigItems() {
		List<String> items = super.getConfigItems();
		items.add("target_addr");
		return items;
	}
	
	public boolean requiresNetwork() {
		return true;
	}
	
	public boolean isAsyncSupported() {
		return false;
	}
	
	public Map<String, String> run() {
		Map<String, String> results = new HashMap<String, String>();
		targetAddr = configs.get("target_addr");
		
		try {
			long lat = adaptor.measureLatency(targetAddr);
			results.put("latency", String.valueOf(lat));
		}
		catch (IOException e) {
			results.put("exception", "IOException: " + e.getMessage());
		}
		
		return results;
	}
}
