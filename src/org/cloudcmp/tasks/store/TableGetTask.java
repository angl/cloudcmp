package org.cloudcmp.tasks.store;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.cloudcmp.Adaptor;
import org.cloudcmp.store.Row;

public class TableGetTask extends StoreTask {	
	private int tableSize;
	private String tableName;
	private String ID;

	public TableGetTask(Adaptor adaptor) {
		super(adaptor);
		configs.put("table_size", "1000");
		configs.put("id", "auto");
		// TODO Auto-generated constructor stub
	}

	public List<String> getConfigItems() {
		List<String> items = super.getConfigItems();
		items.add("table_size");
		items.add("id");
		return items;
	}
	
	public String getTaskName() {
		return "tableget";
	}
	
	public boolean requiresTableStore() {
		return true;
	}
	
	public Map<String, String> run() {
		Map<String, String> results = new HashMap<String, String>();
		tableSize = Integer.parseInt(configs.get("table_size"));
		if (tableSize <= 0) {
			results.put("exception", "wrong table_size value");
			return results;
		}
		
		ID = configs.get("id");
		if (ID == null || ID.equals("auto")) {
			ID = String.valueOf((int)(Math.random() * tableSize));
		}
		
		tableName = "table" + String.valueOf(tableSize);		
		try {
			long startTime = System.nanoTime();
			Row r = adaptor.tableGetRow(tableName, ID);
			if (r == null) { System.err.println("sucks!"); }
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
