package org.cloudcmp.tasks.store;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.cloudcmp.Adaptor;
import org.cloudcmp.store.Row;

public class TableTTCTask extends StoreTask {
	private int tableSize;
	private String tableName;
	private String ID;

	public TableTTCTask(Adaptor adaptor) {
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
		return "tablettc";
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
		
		ID = configs.get("id");
		if (ID == null || ID.equals("auto")) {
			Random rng = new Random();
			ID = String.valueOf(rng.nextLong());
		}
		
		tableName = "table" + String.valueOf(tableSize);
		try {
			adaptor.tableDeleteRow(tableName, ID);			
		}
		catch (Exception e) {}
		
		try {
			Row r = makeRow(ID);
			adaptor.tableInsertRow(tableName, r);
			long startTime = System.nanoTime();
			long endTime = 0;
			boolean found = false;
			boolean inconsistent = false;
			while (!found) {
//				try {
					Row rr = adaptor.tableGetRow(tableName, ID);					
					if (rr != null) {
						found = true;
						endTime = System.nanoTime();
					}
					else {
						inconsistent = true;
					}

			}
			results.put("inconsistent", String.valueOf(inconsistent));
			results.put("ttc", String.valueOf((endTime - startTime) / 1000));
			adaptor.tableDeleteRow(tableName, ID);
		}
		catch (IOException e) {
			results.put("exception", "IOException: " + e.getMessage());
		}
		return results;
	}
}
