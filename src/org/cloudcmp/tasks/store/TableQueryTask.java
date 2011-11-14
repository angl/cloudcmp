package org.cloudcmp.tasks.store;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.cloudcmp.Adaptor;
import org.cloudcmp.store.ColumnType;
import org.cloudcmp.store.Condition;
import org.cloudcmp.store.ConditionType;
import org.cloudcmp.store.Row;

public class TableQueryTask extends StoreTask {
	private int tableSize;
	private String tableName;
	private String value;

	public TableQueryTask(Adaptor adaptor) {
		super(adaptor);
		configs.put("table_size", "1000");
		configs.put("value", "auto");
		// TODO Auto-generated constructor stub
	}

	public List<String> getConfigItems() {
		List<String> items = super.getConfigItems();
		items.add("table_size");
		items.add("value");
		return items;
	}
	
	public String getTaskName() {
		return "tablequery";
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
		
		value = configs.get("value");
		if (value == null || value.equals("auto")) {
			value = String.valueOf((int)(Math.random() * tableSize) / 10);
		}
		
		tableName = "table" + String.valueOf(tableSize);
		try {
			Condition condition = new Condition("nonkey", ConditionType.EQUAL, ColumnType.STRING, value);
			List<Condition> conditions = new ArrayList<Condition>();
			conditions.add(condition);
			long startTime = System.nanoTime();
			List<Row> rows = adaptor.tableQuery(tableName, conditions, null, -1);
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
