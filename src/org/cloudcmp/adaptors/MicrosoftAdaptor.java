package org.cloudcmp.adaptors;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;

import org.cloudcmp.Adaptor;
import org.cloudcmp.store.*;

import org.soyatec.windowsazure.blob.*;
import org.soyatec.windowsazure.blob.io.*;
import org.soyatec.windowsazure.blob.internal.*;
import org.soyatec.windowsazure.error.*;
import org.soyatec.windowsazure.table.*;
import org.soyatec.windowsazure.table.internal.*;
import org.soyatec.windowsazure.queue.*;
import org.soyatec.windowsazure.queue.internal.*;

/* TODO: implement Azure SQL */
public class MicrosoftAdaptor extends Adaptor {
	private IMessage lastMessage = null;
	private TableStorageClient tableClient;
	private BlobStorageClient blobClient;
	private QueueStorageClient queueClient;
	
	public MicrosoftAdaptor() {
		super();
		// default configurations
		configs.put("compute_dollar_per_hour", "0.12"); // price of the default instance
		configs.put("use_https", "true"); // by default, use HTTPS
		configs.put("store_requests_per_cent", "10000");
	}
	
	public String getName() {
		return "Microsoft";
	}
	
	public List<String> getConfigItems() {	
		List<String> items = super.getConfigItems();
		items.add("azure_account_name");
		items.add("azure_account_key");
		items.add("compute_dollar_per_hour");
		items.add("use_https");
		items.add("store_requests_per_cent");
		return items;
	}
	
	public double getComputeCost() {
		return (double)(System.currentTimeMillis()) / 36000 * Double.parseDouble(configs.get("compute_dollar_per_hour")) ;
	}
	
	private void initTableClient() {
		if (Boolean.parseBoolean(configs.get("use_https")))
			tableClient = TableStorageClient.create(URI.create("https://table.core.windows.net"),
					false,
					configs.get("azure_account_name"),
					configs.get("azure_account_key"));
		else
			tableClient = TableStorageClient.create(URI.create("http://table.core.windows.net"),
					false,
					configs.get("azure_account_name"),
					configs.get("azure_account_key"));
	}
	
	private void initBlobClient() {
		if (Boolean.parseBoolean(configs.get("use_https")))
			blobClient = BlobStorageClient.create(URI.create("https://blob.core.windows.net"),
					false,
					configs.get("azure_account_name"),
					configs.get("azure_account_key"));
		else
			blobClient = BlobStorageClient.create(URI.create("http://blob.core.windows.net"),
					false,
					configs.get("azure_account_name"),
					configs.get("azure_account_key"));
	}
	
	private void initQueueClient() {
		if (Boolean.parseBoolean(configs.get("use_https")))
			queueClient = QueueStorageClient.create(URI.create("https://queue.core.windows.net"),
					false,
					configs.get("azure_account_name"),
					configs.get("azure_account_key"));
		else
			queueClient = QueueStorageClient.create(URI.create("http://queue.core.windows.net"),
					false,
					configs.get("azure_account_name"),
					configs.get("azure_account_key"));
	}
	
	public static class AzureStoreEntity extends AbstractTableServiceEntity {
		public AzureStoreEntity(String id) {
			super("", id);
		}
	}
	
	public boolean hasTableStore() {
		return true;
	}
    public void tableCreate(String oriTableName) throws IOException {
    	if (tableClient == null) initTableClient();
		String tableName = Util.getRealTableName(oriTableName);
		try {
			ITable table = tableClient.getTableReference(tableName);
			if (table == null) throw new IOException("TableStorage returned null AzureTable");
			
			if (!table.isTableExist()) table.createTable();
			if (!table.isTableExist()) throw new IOException("Table not created");
		}
		catch (Exception ex) {
			throw new IOException(ex.getMessage());
		}
    }
    public void tableDelete(String oriTableName) throws IOException {
    	if (tableClient == null) initTableClient();
		String tableName = Util.getRealTableName(oriTableName);
		try {
			ITable table = tableClient.getTableReference(tableName);
			if (table == null) throw new IOException("TableStorage returned null AzureTable");
			
			if (table.isTableExist()) {
				table.deleteTable();
			}
			
			if (table.isTableExist()) throw new IOException("Table not deleted");
		}
		catch (StorageException ex) {
			throw new IOException(ex.getMessage());
		}
    }
    public void tableInsertRow(String oriTableName, Row row) throws IOException {
    	if (tableClient == null) initTableClient();
		String tableName = Util.getRealTableName(oriTableName);
		try {
			TableServiceContext table = new TableServiceContext(tableClient.getTableReference(tableName));
			ITableServiceEntity e = new AzureStoreEntity(row.id);
			e.setRowKey(row.id);
			List<ICloudTableColumn> azureColumns = new ArrayList<ICloudTableColumn>();
			
			for (Entry<String, Column> entry : row.columns.entrySet()) {
				ICloudTableColumn azureColumn = new CloudTableColumn();
				azureColumn.setName(entry.getValue().name);
				switch (entry.getValue().type) {
				case STRING:
					azureColumn.setType(ETableColumnType.TYPE_STRING); break;
				case INT:
					azureColumn.setType(ETableColumnType.TYPE_INT); break;
				case DOUBLE:
					azureColumn.setType(ETableColumnType.TYPE_DOUBLE); break;					
				}
				azureColumn.setValue(entry.getValue().value.toString());
				azureColumns.add(azureColumn);
			}
			
			e.setValues(azureColumns);
			table.insertEntity(e);
		}
		catch (Exception ex) {
			throw new IOException(ex.getMessage());
		}
    }
    public void tableInsertRows(String oriTableName, List<Row> rows) throws IOException {
    	if (tableClient == null) initTableClient();
		String tableName = Util.getRealTableName(oriTableName);
		try {
			TableServiceContext table = new TableServiceContext(tableClient.getTableReference(tableName));
			table.startBatch();
			
			for (Row row : rows) {
				ITableServiceEntity e = new AzureStoreEntity(row.id);
				e.setRowKey(row.id);
				List<ICloudTableColumn> azureColumns = new ArrayList<ICloudTableColumn>();
				
				for (Entry<String, Column> entry : row.columns.entrySet()) {
					ICloudTableColumn azureColumn = new CloudTableColumn();
					azureColumn.setName(entry.getValue().name);
					switch (entry.getValue().type) {
					case STRING:
						azureColumn.setType(ETableColumnType.TYPE_STRING); break;
					case INT:
						azureColumn.setType(ETableColumnType.TYPE_INT); break;
					case DOUBLE:
						azureColumn.setType(ETableColumnType.TYPE_DOUBLE); break;					
					}
					azureColumn.setValue(entry.getValue().value.toString());
					azureColumns.add(azureColumn);
				}
				
				e.setValues(azureColumns);
				table.insertEntity(e);
			}
			
			table.executeBatch();
		}
		catch (Exception ex) {
			throw new IOException(ex.getMessage());
		}
    }
    public void tableUpdateRow(String oriTableName, String rowId, List<Column> columns) throws IOException {
    	if (tableClient == null) initTableClient();
		String tableName = Util.getRealTableName(oriTableName);
		try {
			TableServiceContext table = new TableServiceContext(tableClient.getTableReference(tableName));
			ITableServiceEntity olde = new AzureStoreEntity(rowId);
			ITableServiceEntity newe = table.loadEntity(olde);
			
			List<ICloudTableColumn> azureColumns = newe.getValues();
			for (ICloudTableColumn azureColumn : azureColumns) {
				for (Column column : columns) {
					if (azureColumn.getName().equals(column.name)) {
						if (column.value == null) System.out.println(column.name);
						azureColumn.setValue(column.value.toString());
					}
				}
			}
			
			table.updateEntity(newe);
		}
		catch (Exception ex) {
			ex.printStackTrace();
			throw new IOException(ex.getMessage());			
		}
    }
    public Row tableGetRow(String oriTableName, String rowId) throws IOException {
    	if (tableClient == null) initTableClient();
		String tableName = Util.getRealTableName(oriTableName);
		try {
			TableServiceContext table = new TableServiceContext(tableClient.getTableReference(tableName));			
			List<ITableServiceEntity> entities = table.retrieveEntitiesByKey("", rowId, null);
			//List<ITableServiceEntity> entities = table.retrieveEntities(AzureStoreEntity.class);
			
			if (entities.size() == 0) return null;
			Row row = new Row(rowId);
			for (ICloudTableColumn azureColumn : entities.get(0).getValues()) {
				if (azureColumn.getName().equals("PartitionKey")
						|| azureColumn.getName().equals("RowKey")
						|| azureColumn.getName().equals("Timestamp")) continue;
				Column column = new Column();
				column.name = azureColumn.getName();
				switch (azureColumn.getType()) {
				case TYPE_STRING:
					column.value = azureColumn.getValue();
					column.type = ColumnType.STRING;
					break;
				case TYPE_INT:
					column.value = Integer.parseInt(azureColumn.getValue());
					column.type = ColumnType.INT;
					break;
				case TYPE_DOUBLE:
					column.value = Double.parseDouble(azureColumn.getValue());
					column.type = ColumnType.DOUBLE;
					break;
				}
				
				row.columns.put(column.name, column);
			}
			
			return row;			
		}
		catch (Exception ex) {
			throw new IOException(ex.getMessage());
		}
    }
    public Column tableGetColumn(String tableName, String rowId, String columnName) throws IOException {
    	if (tableClient == null) initTableClient();
		try {
			Row row = tableGetRow(tableName, rowId);
			if (row == null) return null;
			
			return row.columns.get(columnName);
		}
		catch (Exception ex) {
			throw new IOException(ex.getMessage());
		}
    }
    public List<Row> tableQuery(String oriTableName, List<Condition> conditions, Order order, int limit) throws IOException {
    	if (tableClient == null) initTableClient();
		String tableName = Util.getRealTableName(oriTableName);
		CloudTableQuery q = getQuery(tableName, conditions, order, limit);
		
		try {
			TableServiceContext table = new TableServiceContext(tableClient.getTableReference(tableName));
			List<Row> rows = new ArrayList<Row>();
			List<ITableServiceEntity> azureEntities = table.retrieveEntities(q, null);
			
			if (azureEntities == null) return null;
			
			for (ITableServiceEntity e : azureEntities) {
				Row row = new Row(e.getRowKey());
				for (ICloudTableColumn azureColumn : e.getValues()) {
					if (azureColumn.getName().equals("PartitionKey")
							|| azureColumn.getName().equals("RowKey")
							|| azureColumn.getName().equals("Timestamp")) continue;
					Column column = new Column();
					column.name = azureColumn.getName();
					switch (azureColumn.getType()) {
					case TYPE_STRING:
						column.value = azureColumn.getValue();
						column.type = ColumnType.STRING;
						break;
					case TYPE_INT:
						column.value = Integer.parseInt(azureColumn.getValue());
						column.type = ColumnType.INT;
						break;
					case TYPE_DOUBLE:
						column.value = Double.parseDouble(azureColumn.getValue());
						column.type = ColumnType.DOUBLE;
						break;
					}
					
					row.columns.put(column.name, column);
				}
				
				rows.add(row);
			}
			
			return rows;		
		}
		catch (Exception ex) {
			throw new IOException(ex.getMessage());
		}
    }
    public int tableCount(String oriTableName, List<Condition> conditions, Order order, int limit) throws IOException {
    	if (tableClient == null) initTableClient();
		String tableName = Util.getRealTableName(oriTableName);
		CloudTableQuery q = getQuery(tableName, conditions, order, limit);
		
		try {
			TableServiceContext table = new TableServiceContext(tableClient.getTableReference(tableName));
			List<ITableServiceEntity> azureEntities = table.retrieveEntities(q, null);
			// there is no "count" operation supported. we retrieve all entries.
			
			if (azureEntities == null) return 0;
			return azureEntities.size();
		}
		catch (Exception ex) {
			throw new IOException(ex.getMessage());
		}
    }
    public void tableDeleteRow(String oriTableName, String rowId) throws IOException {
    	if (tableClient == null) initTableClient();
		String tableName = Util.getRealTableName(oriTableName);
		try {
			TableServiceContext table = new TableServiceContext(tableClient.getTableReference(tableName));
			ITableServiceEntity e = new AzureStoreEntity(rowId);
						
			table.deleteEntity(e);
		}
		catch (Exception ex) {
			throw new IOException(ex.getMessage());
		}
    }
    
	private CloudTableQuery getQuery(String tableName, List<Condition> conditions, Order order, int limit) {
		CloudTableQuery q = CloudTableQuery.select();
		
		if (conditions != null) {
			boolean first = true;
			for (Condition condition : conditions) {
				String name = condition.target.name;
				if (condition.target.isID) name = "RowKey";
							
				CloudTableQuery q2 = CloudTableQuery.select();
				switch (condition.type) {
				case EQUAL:
					q2 = q2.eq(name, condition.target.value);
					break;
				case NOTEQUAL:
					q2 = q2.ne(name, condition.target.value);
					break;
				case LESS:
					q2 = q2.lt(name, condition.target.value);
					break;
				case LESSEQUAL:
					q2 = q2.le(name, condition.target.value);
					break;
				case GREATER:
					q2 = q2.gt(name, condition.target.value);
					break;
				case GREATEREQUAL:
					q2 = q2.ge(name, condition.target.value);
					break;
				}

				if (first) q = q2;
				else q = CloudTableQuery.and(q, q2);
				first = false;
			}
		}
		
		/*
		// so far Azure table does not support "orderby"....
		if (order != null) {
			switch (order.type) {
			case ASC:
				q = q.orderAsc(order.columnName);
				break;
			case DESC:
				q = q.orderDesc(order.columnName);
				break;
			}
		}
		*/

		if (limit > 0)
			q = q.top(limit);

		return q;				
	}
	
    public boolean hasBlobStore() {
    	return true;
    }
    public void blobCreateContainer(String containerName) throws IOException {
    	if (blobClient == null) initBlobClient();
		IBlobContainer container = blobClient.getBlobContainer(containerName);
		try {
			if (container.isContainerExist()) return;
			blobClient.createContainer(containerName);
			if (!container.isContainerExist()) throw new IOException("container not created");
		}
		catch (Exception ex) {
			throw new IOException(ex.getMessage());
		}
    }
    public void blobDeleteContainer(String containerName) throws IOException {
    	if (blobClient == null) initBlobClient();
		IBlobContainer container = blobClient.getBlobContainer(containerName);
		try {
			if (!container.isContainerExist()) return;
			blobClient.deleteContainer(containerName);
		}
		catch (Exception ex) {
			throw new IOException(ex.getMessage());
		}
    }
    public void blobUpload(String containerName, String blobName, byte [] bytes) throws IOException {
    	if (blobClient == null) initBlobClient();
		IBlobContainer container = blobClient.getBlobContainer(containerName);
		try {
			BlobProperties properties = new BlobProperties(blobName);
			BlobContents objContents = new BlobContents(bytes);

			container.createBlockBlob(properties, objContents);
		}
		catch (Exception ex) {
			throw new IOException(ex.getMessage());
		}
    }
    public byte [] blobDownload(String containerName, String blobName, boolean bmark) throws IOException {
    	if (blobClient == null) initBlobClient();
		IBlobContainer container = blobClient.getBlobContainer(containerName);
		try {
			IBlobContents objContents;

			if (bmark) {
				NullStream objStream = new NullStream();
				IBlob blob = container.getBlobReference(blobName);
				objContents = blob.getContents(objStream);
				return objStream.getBytes();
			}
			else {
				BlobMemoryStream objStream = new BlobMemoryStream();
				IBlob blob = container.getBlobReference(blobName);
				objContents = blob.getContents(objStream);
				return objStream.getBytes();
			}
		}
		catch (Exception ex) {
			throw new IOException(ex.getMessage());
		}
    }
    public void blobDelete(String containerName, String blobName) throws IOException {
    	if (blobClient == null) initBlobClient();
		IBlobContainer container = blobClient.getBlobContainer(containerName);
		try {
			container.deleteBlob(blobName);
		}
		catch (Exception ex) {
			throw new IOException(ex.getMessage());
		}
    }
    public boolean blobDoesExist(String containerName, String blobName) throws IOException {
    	if (blobClient == null) initBlobClient();
		IBlobContainer container = blobClient.getBlobContainer(containerName);
		try {
			return container.isBlobExist(blobName);
		}
		catch (Exception ex) {
			throw new IOException(ex.getMessage());
		}
    }
    
    public boolean hasQueueStore() {
    	return true;
    }
    public void queueCreate(String queueName) throws IOException {
    	if (queueClient == null) initQueueClient();
		IQueue queue = queueClient.getQueue(queueName);
		try {
			if (queue.isQueueExist()) return;
			queue.createQueue();
			if (!queue.isQueueExist()) throw new IOException("queue not created");
		}
		catch (Exception ex) {
			throw new IOException(ex.getMessage());
		}
    }
    public void queueDelete(String queueName) throws IOException {
    	if (queueClient == null) initQueueClient();
		IQueue queue = queueClient.getQueue(queueName);
		try {
			if (!queue.isQueueExist()) return;
			queue.deleteQueue();
		}
		catch (Exception ex) {
			throw new IOException(ex.getMessage());
		}
    }
    public void queueClear(String queueName) throws IOException {
    	if (queueClient == null) initQueueClient();
		IQueue queue = queueClient.getQueue(queueName);
		try {
			queue.clear();
		}
		catch (Exception ex) {
			throw new IOException(ex.getMessage());
		}
    }
    public void queuePutMessage(String queueName, String message) throws IOException {
    	if (queueClient == null) initQueueClient();
		IQueue queue = queueClient.getQueue(queueName);
		try {
			queue.putMessage(new Message(message));
		}
		catch (Exception ex) {
			throw new IOException(ex.getMessage());
		}
    }
    public String queueGetMessage(String queueName) throws IOException {
    	if (queueClient == null) initQueueClient();
		IQueue queue = queueClient.getQueue(queueName);
		try {
			lastMessage = queue.getMessage(3600);
			if (lastMessage == null) return null;
			return lastMessage.getContentAsString();
		}
		catch (Exception ex) {
			throw new IOException(ex.getMessage());
		}
    }
    public void queueDeleteLastMessage(String queueName) throws IOException {
    	if (queueClient == null) initQueueClient();
		if (lastMessage == null) return;

		IQueue queue = queueClient.getQueue(queueName);
		try {
			queue.deleteMessage(lastMessage);
		}
		catch (Exception ex) {
			throw new IOException(ex.getMessage());
		}
    }
	
	public static class Util {
	    // get the real table name that conforms to azure's restriction
	    static public String getRealTableName(String name) {
	        String newName = name.replaceAll("_", "ULINE");
	        newName = newName.replaceAll("-", "DASH");
	        return newName;
	    }

	    // get the original table name for application
	    static public String getOriTableName(String name) {
	        String newName = name.replaceAll("ULINE", "_");
	        newName = newName.replaceAll("DASH", "-");
	        return newName;
	    }
	}
	
	public class NullStream implements BlobStream {
	    private long _len;

	    public NullStream() {
	        _len = 0;
	    }

	    public void close() throws IOException {
	    }

	    public long length() throws IOException {
	        return _len;
	    }

	    public long getPosition() throws IOException {
	        return _len;
	    }

	    public int read(byte[] b) throws IOException {
	        return b.length;
	    }

	    public void write(byte[] buffer, int off, int len) throws IOException {
	        _len += len;
	    }

	    public boolean canSeek() {
	        return true;
	    }

	    public byte[] getBytes() throws IOException {
	        byte[] buf = new byte[1];
	        return buf;
	    }
	    public void setPosition(long position) throws IOException {
	        _len = position;
	    }

	    public int read(byte[] buffer, int offset, int len) throws IOException {
	        return buffer.length;
	    }
	}
	
	public double getLastOperationCost() {
		return 1.0 / Double.parseDouble(configs.get("store_requests_per_cent"));
	}
}
