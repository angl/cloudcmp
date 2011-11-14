package org.cloudcmp.adaptors;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;

import org.cloudcmp.Adaptor;
import org.cloudcmp.store.*;

import com.google.appengine.api.datastore.*;
import com.google.appengine.api.datastore.Query.FilterOperator;
import com.google.appengine.api.datastore.Query.SortDirection;
import com.google.appengine.api.datastore.ReadPolicy.Consistency;
import com.google.appengine.api.quota.QuotaService;
import com.google.appengine.api.quota.QuotaServiceFactory;

public class GoogleAdaptor extends Adaptor {
	/* google-specific state */
	private DatastoreService datastore = null;
	
	public GoogleAdaptor() {
		super();
		configs.put("compute_dollar_per_hour", "0.1");
		configs.put("consistency", "false"); // by default, no consistency guarantee
		configs.put("datastore_dollar_per_hour", "0.1");
	}
	
	public String getName() {
		return "Google";
	}
	
	public List<String> getConfigItems() {	
		List<String> items = super.getConfigItems();
		items.add("compute_dollar_per_hour");
		items.add("consistency");
		items.add("datastore_dollar_per_hour");
		return items;
	}
	
	public boolean supportMultipleThreads() {
		return false; // GAE does not support multithreading for now
	}
	
	public double getComputeCost() {
		QuotaService qs = QuotaServiceFactory.getQuotaService();
		double cpuSeconds = qs.convertMegacyclesToCpuSeconds(qs.getCpuTimeInMegaCycles());		
    	return cpuSeconds / 3600 * Double.parseDouble(configs.get("compute_dollar_per_hour")) * 100;
	}
	
	private void initDataStore() {
		if (Boolean.parseBoolean(configs.get("consistency")))
			datastore = DatastoreServiceFactory.getDatastoreService(
					DatastoreServiceConfig.Builder.withReadPolicy(new ReadPolicy(Consistency.STRONG)));
		else
			datastore = DatastoreServiceFactory.getDatastoreService(
                    DatastoreServiceConfig.Builder.withReadPolicy(new ReadPolicy(Consistency.EVENTUAL)));
	}
	
	private PreparedQuery prepareQuery(String tableName, List<Condition> conditions,
			Order order) {
		Query q = new Query(tableName);
				
		if (conditions != null) {
			for (Condition condition : conditions) {
				String property;
				if (condition.target.isID)
					property = "__key__";
				else
					property = condition.target.name;
				
				switch (condition.type) {
				case EQUAL:
					q.addFilter(property, FilterOperator.EQUAL, condition.target.value); break;
				case NOTEQUAL:
					q.addFilter(property, FilterOperator.NOT_EQUAL, condition.target.value); break;
				case LESS:
					q.addFilter(property, FilterOperator.LESS_THAN, condition.target.value); break;
				case LESSEQUAL:
					q.addFilter(property, FilterOperator.LESS_THAN_OR_EQUAL, condition.target.value); break;
				case GREATER:
					q.addFilter(property, FilterOperator.GREATER_THAN, condition.target.value); break;
				case GREATEREQUAL:
					q.addFilter(property, FilterOperator.GREATER_THAN_OR_EQUAL, condition.target.value); break;
				}				
			}
		}
		
		if (order != null) {
			switch (order.type) {
			case ASC:			
				q.addSort(order.columnName, SortDirection.ASCENDING); break;
			case DESC:
				q.addSort(order.columnName, SortDirection.DESCENDING); break;
			}
		}
		
		return datastore.prepare(q);
	}
	
	public boolean hasTableStore() {
		return true;
	}
    //public void tableCreate(String tableName) throws IOException {} // there is no need to explicitly create a table in GAE
    //public void tableDelete(String tableName) throws IOException {}
    public void tableInsertRow(String tableName, Row row) throws IOException {
    	if (datastore == null) initDataStore();
		Key k = KeyFactory.createKey(tableName, row.id);
		Entity e = new Entity(k);
		
		for (Entry<String, Column> entry : row.columns.entrySet()) {
			e.setProperty(entry.getValue().name, entry.getValue().value);
		}
		
		try {
			datastore.put(e);
		}
		catch (DatastoreFailureException ex) {
			throw new IOException(ex.getMessage());
		}
    }
    public void tableInsertRows(String tableName, List<Row> rows) throws IOException {
    	if (datastore == null) initDataStore();
		List<Entity> es = new ArrayList<Entity>();
		
		for (Row row : rows) {
			Key k = KeyFactory.createKey(tableName, row.id);
			Entity e = new Entity(k);
			
			for (Entry<String, Column> entry : row.columns.entrySet()) {
				e.setProperty(entry.getValue().name, entry.getValue().value);
			}
			
			es.add(e);
		}
		
		try {
			datastore.put(es);
		}
		catch (DatastoreFailureException ex) {
			throw new IOException(ex.getMessage());
		}
    }
    public void tableUpdateRow(String tableName, String rowId, List<Column> columns) throws IOException {
    	if (datastore == null) initDataStore();
		Key k = KeyFactory.createKey(tableName, rowId);
		Entity e = new Entity(k);
		
		for (Column column : columns) {
			e.setProperty(column.name, column.value);
		}
		
		try {
			datastore.put(e);
		}
		catch (DatastoreFailureException ex) {
			throw new IOException(ex.getMessage());
		}
    }
    public Row tableGetRow(String tableName, String rowId) throws IOException {
    	if (datastore == null) initDataStore();
		Key k = KeyFactory.createKey(tableName, rowId);
		
		try {
			Entity e = datastore.get(k);
			Row row = new Row(rowId);
			for (Entry<String, Object> entry : e.getProperties().entrySet()) {
				Column column = new Column();
				column.name = entry.getKey();
				column.value = entry.getValue();
				
				if (column.value instanceof String)
					column.type = ColumnType.STRING;
				else if (column.value instanceof Integer)
					column.type = ColumnType.INT;
				else if (column.value instanceof Double)
					column.type = ColumnType.DOUBLE;
				else if (column.value instanceof Long) {
					column.type = ColumnType.INT;
					column.value = new Integer(((Long)column.value).intValue());
				}

				row.columns.put(column.name, column);
			}
			return row;
		}
		catch (EntityNotFoundException ex) {
			return null;
		}
		catch (DatastoreFailureException ex) {
			throw new IOException(ex.getMessage());
		}
    }
    public Column tableGetColumn(String tableName, String rowId, String columnName) throws IOException {
    	if (datastore == null) initDataStore();
		Key k = KeyFactory.createKey(tableName, rowId);
		
		try {
			Entity e = datastore.get(k);			
			if (e.getProperty(columnName) == null) return null;
			
			Column column = new Column();
			column.name = columnName;
			column.value = e.getProperty(columnName);
		
			if (column.value instanceof String)
				column.type = ColumnType.STRING;
			else if (column.value instanceof Integer)
				column.type = ColumnType.INT;
			else if (column.value instanceof Double)
				column.type = ColumnType.DOUBLE;
			else if (column.value instanceof Long) {
				column.type = ColumnType.INT;
				column.value = new Integer(((Long)column.value).intValue());
			}
				
			return column;	
		}
		catch (EntityNotFoundException ex) {
			return null;
		}
		catch (DatastoreFailureException ex) {
			throw new IOException(ex.getMessage());
		}
    }
    public List<Row> tableQuery(String tableName, List<Condition> conditions, Order order, int limit) throws IOException {
    	if (datastore == null) initDataStore();
		try {
			PreparedQuery pq = prepareQuery(tableName, conditions, order);
			if (pq == null) return null;
			
			List<Row> rows = new ArrayList<Row>();
			Iterator<Entity> es;
			
			if (limit > 0) es = pq.asIterator(FetchOptions.Builder.withLimit(limit));
			else es = pq.asIterator();
			
			while (es.hasNext()) {
				Entity e = es.next();
				Row row = new Row(e.getKey().getName());
				
				for (Entry<String, Object> entry : e.getProperties().entrySet()) {
					Column column = new Column();
					column.name = entry.getKey();
					column.value = entry.getValue();
					
					if (column.value instanceof String)
						column.type = ColumnType.STRING;
					else if (column.value instanceof Integer)
						column.type = ColumnType.INT;
					else if (column.value instanceof Double)
						column.type = ColumnType.DOUBLE;
					else if (column.value instanceof Long) {
						column.type = ColumnType.INT;
						column.value = new Integer(((Long)column.value).intValue());
					}
					row.columns.put(column.name, column);
				}
				
				rows.add(row);
			}
			
			return rows;
		}
		catch (DatastoreFailureException ex) {
			throw new IOException(ex.getMessage());
		}
    }
    public int tableCount(String tableName, List<Condition> conditions, Order order, int limit) throws IOException {
    	if (datastore == null) initDataStore();
		try {
			PreparedQuery pq = prepareQuery(tableName, conditions, order);
			if (pq == null) return 0;
			
			return pq.countEntities(FetchOptions.Builder.withDefaults()); // maximum 1000
		}
		catch (DatastoreFailureException ex) {
			throw new IOException(ex.getMessage());
		}
    }
    public void tableDeleteRow(String tableName, String rowId) throws IOException {
    	if (datastore == null) initDataStore();
		Key k = KeyFactory.createKey(tableName, rowId);
		
		try {
			datastore.delete(k);
		}
		catch (DatastoreFailureException ex) {
			throw new IOException(ex.getMessage());
		}	
    }
    
    public double getLastOperationCost() {
		QuotaService qs = QuotaServiceFactory.getQuotaService();
		double cpuSeconds = qs.convertMegacyclesToCpuSeconds(qs.getApiTimeInMegaCycles());		
    	return cpuSeconds / 3600 * Double.parseDouble(configs.get("datastore_dollar_per_hour")) * 100;
    }
    
    public boolean hasNetwork() {
    	return false;
    }
}
