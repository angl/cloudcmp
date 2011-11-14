package org.cloudcmp.tasks.store;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.cloudcmp.Adaptor;
import org.cloudcmp.store.Row;

public class TablePutTask extends StoreTask {
	private boolean keep;
	private int tableSize;
	private String tableName;
	private String ID;

	public TablePutTask(Adaptor adaptor) {
		super(adaptor);
		configs.put("table_size", "1000");
		configs.put("id", "auto");
		configs.put("keep", "false");
		// TODO Auto-generated constructor stub
	}

	public List<String> getConfigItems() {
		List<String> items = super.getConfigItems();
		items.add("table_size");
		items.add("id");
		items.add("keep");
		return items;
	}
	
	public String getTaskName() {
		return "tableput";
	}
	
	public boolean requiresTableStore() {
		return true;
	}
	
	private Row makeRow(String id) {
		Row r = new Row(id);
		String nonkey = String.valueOf((int)(Math.random() * tableSize / 10));
		StringBuilder randoms = new StringBuilder();
		for (int i = 0; i < PopulateTableTask.DATA_LENGTH; ++ i) {
			randoms.append((char)((int)(Math.random() * 26) + 65));
		}
		r.setString("nonkey", nonkey);
		r.setString("data", randoms.toString());
		return r;
	}
	
	public Map<String, String> run() {
		Map<String, String> results = new HashMap<String, String>();
		tableSize = Integer.parseInt(configs.get("table_size"));
		if (tableSize <= 0) {
			results.put("exception", "wrong table_size value");
			return results;
		}
		
		keep = Boolean.parseBoolean(configs.get("keep"));		
		
		ID = configs.get("id");
		if (ID == null || ID.equals("auto")) {
			Random rng = new Random();
			ID = String.valueOf(rng.nextLong());
		}
		
		tableName = "table" + String.valueOf(tableSize);
		try {
			Row r = makeRow(ID);
			long startTime = System.nanoTime();
			adaptor.tableInsertRow(tableName, r);
			long endTime = System.nanoTime();
			results.put("time", String.valueOf((endTime - startTime) / 1000));
			results.put("cost", String.valueOf(adaptor.getLastOperationCost()));
			
			if (!keep) adaptor.tableDeleteRow(tableName, ID);
		}
		catch (IOException e) {
			results.put("exception", "IOException: " + e.getMessage());
		}
		return results;
	}
}
