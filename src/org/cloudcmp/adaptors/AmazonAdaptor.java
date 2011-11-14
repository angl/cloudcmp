package org.cloudcmp.adaptors;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.cloudcmp.Adaptor;
import org.cloudcmp.store.*;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.*;
import com.amazonaws.services.simpledb.*;
import com.amazonaws.services.simpledb.model.*;
import com.amazonaws.services.s3.*;
import com.amazonaws.services.s3.model.*;
import com.amazonaws.services.sqs.*;
import com.amazonaws.services.sqs.model.*;

public class AmazonAdaptor extends Adaptor {
	/* amazon-specific constants */
	private final static String INT_FORMAT_STRING = "%010d";
	private final static String DOUBLE_FORMAT_STRING = "%019.8f";
	private final static int INT_OFFSET = 1000000000;
	
	/* amazon-specific state */	
	private AmazonSimpleDBClient tableClient = null;
	private AmazonS3Client blobClient = null;
	private AmazonSQSClient queueClient = null;
	private double lastCost = 0;
	private static Map<String, String> urlMap = new HashMap<String, String>();
	private String lastHandle = null;
	
	public AmazonAdaptor() {
		super();
		// default configurations are put here
		configs.put("compute_dollar_per_hour", "0.085"); // price of the default "small" instance
		configs.put("use_https", "true"); // by default, use HTTPS
		configs.put("consistency", "0"); // by default, no consistency guarantee		
		configs.put("simpledb_dollar_per_hour", "0.14");
		configs.put("s3_put_requests_per_cent", "1000");
		configs.put("s3_get_requests_per_cent", "10000");
		configs.put("sqs_requests_per_cent", "10000");
	}
	
	public String getName() {
		return "Amazon";
	}
	
	public List<String> getConfigItems() {	
		List<String> items = super.getConfigItems();
		items.add("access_key_id");
		items.add("secret_access_key");
		items.add("compute_dollar_per_hour");
		items.add("use_https");
		items.add("consistency");
		items.add("simpledb_dollar_per_hour");
		items.add("s3_put_requests_per_cent");
		items.add("s3_get_requests_per_cent");
		items.add("sqs_requests_per_cent");
		
		return items;
	}
	
	public double getComputeCost() {
		return (double)(System.currentTimeMillis()) / 36000 * Double.parseDouble(configs.get("compute_dollar_per_hour")) ;
	}
	
	private void initTableClient() {
		BasicAWSCredentials cred = new BasicAWSCredentials(configs.get("access_key_id"), configs.get("secret_access_key"));
        tableClient = new AmazonSimpleDBClient(cred);
		if (Boolean.parseBoolean(configs.get("use_https")))
			tableClient.setEndpoint("https://sdb.amazonaws.com");
		else
			tableClient.setEndpoint("http://sdb.amazonaws.com");
	}
	
	private void initBlobClient() {
		BasicAWSCredentials cred = new BasicAWSCredentials(configs.get("access_key_id"), configs.get("secret_access_key"));
		blobClient = new AmazonS3Client(cred);
		if (Boolean.parseBoolean(configs.get("use_https")))
			blobClient.setEndpoint("https://s3.amazonaws.com");
		else
			blobClient.setEndpoint("http://s3.amazonaws.com");
	}
	
	private void initQueueClient() {
		BasicAWSCredentials cred = new BasicAWSCredentials(configs.get("access_key_id"), configs.get("secret_access_key"));
		queueClient = new AmazonSQSClient(cred);
		if (Boolean.parseBoolean(configs.get("use_https")))
			queueClient.setEndpoint("https://queue.amazonaws.com");
		else
			queueClient.setEndpoint("http://queue.amazonaws.com");
	}
	
	private double computeTableCost(double boxUsage) {
		return boxUsage * Double.parseDouble(configs.get("simpledb_dollar_per_hour")) * 100;
	}
	
    private SelectResult select(String tableName, List<Condition> conditions,
            Order order, int limit, boolean count, String nextToken) throws AmazonClientException{
    	if (tableClient == null) initTableClient();
        boolean needOrderCondition = false;
        SelectRequest req = new SelectRequest();
        req.setConsistentRead(Integer.parseInt(configs.get("consistency")) == 1?true:false);
        if (nextToken != null)
            req.setNextToken(nextToken);

        StringBuilder selectBuilder = new StringBuilder();
        if (count)
            selectBuilder.append("select count(*)");
        else
            selectBuilder.append("select *");

        selectBuilder.append(" from `" + tableName + "`");

        if (order != null) {
            // add the order field to condition if not exist
            boolean found = false;

            if (conditions != null) {
                for (Condition condition : conditions) {
                    if (condition.target.name.equals(order.columnName)) {
                        found = true;
                        break;
                    }
                }
            }
            
            needOrderCondition = !found;
        }

        if (conditions != null) {
            selectBuilder.append(" where");
            boolean first = true;
            for (Condition condition : conditions) {
                if (!first) selectBuilder.append(" and");
                if (condition.target.isID)
                    selectBuilder.append(" itemName()");
                else
                    selectBuilder.append(" `" + condition.target.name + "`");
                switch (condition.type) {
                case EQUAL:
                    selectBuilder.append(" ="); break;
                case NOTEQUAL:
                    selectBuilder.append(" !="); break;
                case LESS:
                    selectBuilder.append(" <"); break;
                case LESSEQUAL:
                    selectBuilder.append(" <="); break;
                case GREATER:
                    selectBuilder.append(" >"); break;
                case GREATEREQUAL:
                    selectBuilder.append(" >="); break;
                }

                selectBuilder.append(" '" + Util.convertColumn2String(condition.target) + "'");

                first = false;
            }
        }
        
        if (order != null) {
            if (needOrderCondition) {
                if (conditions == null) {
                    selectBuilder.append(" where");
                }
                else {
                    selectBuilder.append(" and");
                }

                selectBuilder.append(" `" + order.columnName + "` != ''");
            }

            selectBuilder.append(" order by");
            selectBuilder.append(" `" + order.columnName + "`");
            switch (order.type) {
            case ASC:
                selectBuilder.append(" asc"); break;
            case DESC:
                selectBuilder.append(" desc"); break;
            }
        }

        if (limit > 0) {
            selectBuilder.append(" limit");
            selectBuilder.append(" " + limit);
        }

        req.setSelectExpression(selectBuilder.toString());

        try {
            SelectResult res = tableClient.select(req);
            SimpleDBResponseMetadata metadata = tableClient.getCachedResponseMetadata(req);
            lastCost = computeTableCost((double)metadata.getBoxUsage());
            return res;
        }
        catch (AmazonClientException ex) {
            throw ex;
        }
    }
    
	public boolean hasTableStore() {
		return true;
	}
	
    public void tableCreate(String tableName) throws IOException {
    	if (tableClient == null) initTableClient();
        CreateDomainRequest req = new CreateDomainRequest();
        req.setDomainName(tableName);

        try {
        	tableClient.createDomain(req);
        	SimpleDBResponseMetadata metadata = tableClient.getCachedResponseMetadata(req);
            lastCost = computeTableCost((double)(metadata.getBoxUsage()));
        }
        catch (AmazonClientException ex) {
            throw new IOException(ex.getMessage());
        }
    }    
    public void tableDelete(String tableName) throws IOException {
    	if (tableClient == null) initTableClient();
        DeleteDomainRequest req = new DeleteDomainRequest();
        req.setDomainName(tableName);

        try {
        	tableClient.deleteDomain(req);
        	SimpleDBResponseMetadata metadata = tableClient.getCachedResponseMetadata(req);
            lastCost = computeTableCost(metadata.getBoxUsage());
        }
        catch (AmazonClientException ex) {
            throw new IOException(ex.getMessage());
        }
    }
    public void tableInsertRow(String tableName, Row row) throws IOException {
    	if (tableClient == null) initTableClient();
        PutAttributesRequest req = new PutAttributesRequest();
        req.setDomainName(tableName);
        req.setItemName(row.id);

        List<ReplaceableAttribute> attributes = new ArrayList<ReplaceableAttribute>();
        for (Map.Entry<String, Column> entry : row.columns.entrySet()) {
            ReplaceableAttribute attr = new ReplaceableAttribute();
            attr.setName(entry.getValue().name);
            attr.setReplace(false);
            attr.setValue(Util.convertColumn2String(entry.getValue()));
            attributes.add(attr);
        }

        req.setAttributes(attributes);

        try {
        	tableClient.putAttributes(req);
        	SimpleDBResponseMetadata metadata = tableClient.getCachedResponseMetadata(req);
            lastCost = computeTableCost(metadata.getBoxUsage());
        }
        catch (AmazonClientException ex) {
            throw new IOException(ex.getMessage());
        }
    }
    public void tableInsertRows(String tableName, List<Row> rows) throws IOException {
    	if (tableClient == null) initTableClient();
        BatchPutAttributesRequest req = new BatchPutAttributesRequest();
        req.setDomainName(tableName);

        List<ReplaceableItem> items = new ArrayList<ReplaceableItem>();
        for (Row row : rows) {
            ReplaceableItem item = new ReplaceableItem();
            item.setName(row.id);
            List<ReplaceableAttribute> attributes = new ArrayList<ReplaceableAttribute>();

            for (Map.Entry<String, Column> entry : row.columns.entrySet()) {
                ReplaceableAttribute attr = new ReplaceableAttribute();
                attr.setName(entry.getValue().name);
                attr.setReplace(false);
                attr.setValue(Util.convertColumn2String(entry.getValue()));
                attributes.add(attr);
            }

            item.setAttributes(attributes);
            items.add(item);
        }

        req.setItems(items);

        try {
        	tableClient.batchPutAttributes(req);
        	SimpleDBResponseMetadata metadata = tableClient.getCachedResponseMetadata(req);
            lastCost = computeTableCost(metadata.getBoxUsage());
        }
        catch (AmazonClientException ex) {
            throw new IOException(ex.getMessage());
        }
    }
    public void tableUpdateRow(String tableName, String rowId, List<Column> columns) throws IOException {
    	if (tableClient == null) initTableClient();
        PutAttributesRequest req = new PutAttributesRequest();
        req.setDomainName(tableName);
        req.setItemName(rowId);

        List<ReplaceableAttribute> attributes = new ArrayList<ReplaceableAttribute>();
        for (Column column : columns) {
            ReplaceableAttribute attr = new ReplaceableAttribute();
            attr.setName(column.name);
            attr.setReplace(true);
            attr.setValue(Util.convertColumn2String(column));
            attributes.add(attr);
        }

        req.setAttributes(attributes);

        try {
        	tableClient.putAttributes(req);
        	SimpleDBResponseMetadata metadata = tableClient.getCachedResponseMetadata(req);
            lastCost = computeTableCost(metadata.getBoxUsage());
        }
        catch (AmazonClientException ex) {
            throw new IOException(ex.getMessage());
        }    	
    }
    public Row tableGetRow(String tableName, String rowId) throws IOException {
    	if (tableClient == null) initTableClient();
        GetAttributesRequest req = new GetAttributesRequest();
        req.setDomainName(tableName);
        req.setItemName(rowId);
        req.setConsistentRead(Integer.parseInt(configs.get("consistency")) == 1?true:false);

        try {
            GetAttributesResult result = tableClient.getAttributes(req);
            SimpleDBResponseMetadata metadata = tableClient.getCachedResponseMetadata(req);
            lastCost = computeTableCost(metadata.getBoxUsage());
            if (result == null || result.getAttributes().size() == 0) return null;

            Row row = new Row(rowId);
            for (Attribute attr : result.getAttributes()) {
                Column column = Util.convertString2Column(attr.getName(), attr.getValue());
                row.columns.put(column.name, column);
            }

            return row;
        }
        catch (AmazonClientException ex) {
            throw new IOException(ex.getMessage());
        }
    }
    public Column tableGetColumn(String tableName, String rowId, String columnName) throws IOException {
    	if (tableClient == null) initTableClient();
        GetAttributesRequest req = new GetAttributesRequest();
        req.setDomainName(tableName);
        req.setItemName(rowId);
        req.setConsistentRead(Integer.parseInt(configs.get("consistency")) == 1?true:false);
        List<String> attributes = new ArrayList<String>();

        attributes.add(columnName);
        req.setAttributeNames(attributes);

        try {
            GetAttributesResult result = tableClient.getAttributes(req);
            SimpleDBResponseMetadata metadata = tableClient.getCachedResponseMetadata(req);
            lastCost = computeTableCost(metadata.getBoxUsage());
            if (result == null || result.getAttributes().size() == 0) return null;

            Attribute attr = result.getAttributes().get(0);
            return Util.convertString2Column(attr.getName(), attr.getValue());
        }
        catch (AmazonClientException ex) {
            throw new IOException(ex.getMessage());
        }
    }
    public List<Row> tableQuery(String tableName, List<Condition> conditions, Order order, int limit) throws IOException {
    	if (tableClient == null) initTableClient();
        try {
            List<Row> rows = new ArrayList<Row>();
            String nextToken = null;

            do {
                SelectResult result = select(tableName, conditions, order, limit, false, nextToken);
                nextToken = result.getNextToken();

                List<Item> items = result.getItems();
                for (Item item : items) {
                    Row row = new Row(item.getName());
                    for (Attribute attr : item.getAttributes()) {
                        Column column = Util.convertString2Column(attr.getName(), attr.getValue());
                        row.columns.put(column.name, column);
                    }

                    rows.add(row);
                }
            } while (nextToken != null && (rows.size() < limit || limit == -1));

            return rows;
        }
        catch (AmazonClientException ex) {
            throw new IOException(ex.getMessage());
        }
    }
    public int tableCount(String tableName, List<Condition> conditions, Order order, int limit) throws IOException {
    	if (tableClient == null) initTableClient();
		try {
			int count = 0;
			String nextToken = null;
			
			do {		
				SelectResult result = select(tableName, conditions, order, limit, true, nextToken);
				nextToken = result.getNextToken();
				
				List<Item> items = result.getItems();
				count += Integer.parseInt(items.get(0).getAttributes().get(0).getValue());
			} while (nextToken != null);
			
			return count;
		}
		catch (AmazonClientException ex) {
			throw new IOException(ex.getMessage());
		}
    }
    public void tableDeleteRow(String tableName, String rowId) throws IOException {
    	if (tableClient == null) initTableClient();
		DeleteAttributesRequest req = new DeleteAttributesRequest();
		req.setItemName(rowId);
		req.setDomainName(tableName);
		
		try {
			tableClient.deleteAttributes(req);
			SimpleDBResponseMetadata metadata = tableClient.getCachedResponseMetadata(req);
			lastCost = computeTableCost(metadata.getBoxUsage());
		}
		catch (AmazonClientException ex) {
			throw new IOException(ex.getMessage());
		}
    }
    
    private double computeBlobGetCost() {
    	return 1.0 / Double.parseDouble(configs.get("s3_get_requests_per_cent"));
    }
    
    private double computeBlobPutCost() {
    	return 1.0 / Double.parseDouble(configs.get("s3_put_requests_per_cent"));
    }

    public boolean hasBlobStore() {
    	return true;
    }
    
    public void blobCreateContainer(String containerName) throws IOException {
    	if (blobClient == null) initBlobClient();
		try {
			String realContainerName = "blobstore-" + containerName.toLowerCase();
			blobClient.createBucket(realContainerName);
			lastCost = computeBlobPutCost();
		}
		catch (Exception ex) {
			throw new IOException(ex.getMessage());
		}  	
    }
    public void blobDeleteContainer(String containerName) throws IOException {
    	if (blobClient == null) initBlobClient();
		try {
			String realContainerName = "blobstore-" + containerName.toLowerCase();
			blobClient.deleteBucket(realContainerName);
			lastCost = 0;
		}
		catch (Exception ex) {
			throw new IOException(ex.getMessage());
		}
    }
    public void blobUpload(String containerName, String blobName, byte [] bytes) throws IOException {
    	if (blobClient == null) initBlobClient();
		String realContainerName = "blobstore-" + containerName.toLowerCase();

		try {
			InputStream is = new ByteArrayInputStream(bytes);
			blobClient.putObject(realContainerName, blobName, is, null);
			lastCost = computeBlobPutCost();
		}
		catch (Exception ex) {
			throw new IOException(ex.getMessage());
		}
    }
    public byte [] blobDownload(String containerName, String blobName, boolean bmark) throws IOException {
    	if (blobClient == null) initBlobClient();
		byte [] result = null;

		String realContainerName = "blobstore-" + containerName.toLowerCase();
		try {
			S3Object obj = blobClient.getObject(realContainerName, blobName);
			InputStream is = obj.getObjectContent();
			int len = (int)(obj.getObjectMetadata().getContentLength());
			if (bmark) // if benchmarking, use a fixed-size buffer (8MB)
				result = new byte[8000000];
			else
				result = new byte[len];

			if (bmark) {
				// simply drain the stream
				int count = 0;
				while ((count = is.read(result)) > 0) {
				}
			}
			else {
				int off = 0;
				int count = 0;
				while ((count = is.read(result, off, len - off)) > 0) {
					off += count;
				}
			}

			is.close();
			lastCost = computeBlobGetCost();
		}
		catch (Exception ex) {
			throw new IOException(ex.getMessage());
		}

		return result;
    }
    public void blobDelete(String containerName, String blobName) throws IOException {
    	if (blobClient == null) initBlobClient();
		String realContainerName = "blobstore-" + containerName.toLowerCase();
		try {
			blobClient.deleteObject(realContainerName, blobName);
			lastCost = 0;
		}
		catch (Exception ex) {
			throw new IOException(ex.getMessage());
		}
    }
    public boolean blobDoesExist(String containerName, String blobName) throws IOException {
    	if (blobClient == null) initBlobClient();
		String realContainerName = "blobstore-" + containerName.toLowerCase();
		try {
			ObjectMetadata md = blobClient.getObjectMetadata(realContainerName, blobName);
			lastCost = computeBlobPutCost();
			if (md != null) return true;
			else return false;
		}
		catch (Exception ex) {
			throw new IOException(ex.getMessage());
		}
    }
    
	private String getQueueUrl(String queueName) throws IOException {
		if (urlMap.containsKey(queueName))
			return urlMap.get(queueName);

		try {
			ListQueuesRequest req = new ListQueuesRequest();
			req.setQueueNamePrefix(queueName);
			ListQueuesResult result = queueClient.listQueues(req);
			String url = result.getQueueUrls().get(0);
			urlMap.put(queueName, url);

			return url;
		}
		catch (Exception ex) {
			throw new IOException(ex.getMessage());
		}
	}
	
	private double computeQueueCost() {
		return 1.0 / Double.parseDouble(configs.get("sqs_requests_per_cent"));
	}
    
    public boolean hasQueueStore() {
    	return true;
    }
    
    public void queueCreate(String queueName) throws IOException {
    	if (queueClient == null) initQueueClient();
		try {
			CreateQueueRequest req = new CreateQueueRequest();
			req.setQueueName(queueName);
			CreateQueueResult result = queueClient.createQueue(req);
			urlMap.put(queueName, result.getQueueUrl());
			lastCost = computeQueueCost();
		}
		catch (Exception ex) {
			throw new IOException(ex.getMessage());
		}
    }
    public void queueDelete(String queueName) throws IOException {
    	if (queueClient == null) initQueueClient();
		try {
			String url = getQueueUrl(queueName);
			DeleteQueueRequest req = new DeleteQueueRequest();
			req.setQueueUrl(url);
			queueClient.deleteQueue(req);
			lastCost = computeQueueCost();
		}
		catch (Exception ex) {
			throw new IOException(ex.getMessage());
		}
    }
    public void queueClear(String queueName) throws IOException {
    	if (queueClient == null) initQueueClient();
		try {
			String url = getQueueUrl(queueName);
			ReceiveMessageRequest req = new ReceiveMessageRequest();
			req.setQueueUrl(url);
			req.setMaxNumberOfMessages(new Integer(10));
			req.setVisibilityTimeout(0);
			ReceiveMessageResult result = queueClient.receiveMessage(req);
			lastCost = computeQueueCost();

			for (Message msg : result.getMessages()) {
				DeleteMessageRequest req2 = new DeleteMessageRequest();
				req2.setQueueUrl(url);
				req2.setReceiptHandle(msg.getReceiptHandle());
				queueClient.deleteMessage(req2);
				lastCost += computeQueueCost();
			}
		}
		catch (Exception ex) {
			throw new IOException(ex.getMessage());
		}    
    }
    public void queuePutMessage(String queueName, String message) throws IOException {
    	if (queueClient == null) initQueueClient();
		SendMessageRequest req = new SendMessageRequest();
		String url = getQueueUrl(queueName);
		req.setMessageBody(message);
		req.setQueueUrl(url);

		try {
			queueClient.sendMessage(req);
			lastCost = computeQueueCost();
		}
		catch (Exception ex) {
			throw new IOException(ex.getMessage());
		}
    }
    public String queueGetMessage(String queueName) throws IOException {
    	if (queueClient == null) initQueueClient();
		ReceiveMessageRequest req = new ReceiveMessageRequest();
		req.setQueueUrl(getQueueUrl(queueName));
		req.setMaxNumberOfMessages(1);
		req.setVisibilityTimeout(43200);

		try {
			ReceiveMessageResult result = queueClient.receiveMessage(req);
			if (result.getMessages() == null || result.getMessages().size() == 0) return null;

			Message msg = result.getMessages().get(0);
			lastHandle = msg.getReceiptHandle();
			lastCost = computeQueueCost();
			return msg.getBody();
		}
		catch (Exception ex) {
			throw new IOException(ex.getMessage());
		}
    }
    public void queueDeleteLastMessage(String queueName) throws IOException {
    	if (queueClient == null) initQueueClient();
		if (lastHandle == null) return;

		DeleteMessageRequest req = new DeleteMessageRequest();
		req.setQueueUrl(getQueueUrl(queueName));
		req.setReceiptHandle(lastHandle);
		try {
			queueClient.deleteMessage(req);
			lastCost = computeQueueCost();
		}
		catch (Exception ex) {
			throw new IOException(ex.getMessage());
		}
    }
    
    public static class Util {
        // convert a column to a string stored in simpleDB
        public static String convertColumn2String(Column column) {
            char type = '\0';
            String result = "";

            switch (column.type) {
            case STRING:
                type = 's';
                result = (column.value == null? "" : column.value.toString());
                break;
            case INT:
                type = 'i';
                int value;
                if (column.value == null)
                    value = INT_OFFSET; // we don't really distinguish between zero and null
                else
                    value = ((Integer)column.value).intValue() + INT_OFFSET;
                result = String.format(INT_FORMAT_STRING, value);
                break;
            case DOUBLE:
                type = 'd';
                double value2;
                if (column.value == null)
                    value2 = INT_OFFSET;
                else
                    value2 = ((Double)column.value).doubleValue() + INT_OFFSET;
                result = String.format(DOUBLE_FORMAT_STRING, value2);
                break;
            }

            return type + result;
        }
        
        // convert a string stored in simpleDB to a column
        public static Column convertString2Column(String name, String data) {
            if (data == null || data.length() == 0) return null;
            char type = data.charAt(0);

            switch (type) {
            case 's':
                return new Column(name, data.substring(1));
            case 'i':
                Column ret = new Column();
                ret.name = name;
                ret.type = ColumnType.INT;
                ret.value = new Integer(Integer.parseInt(data.substring(1)) - INT_OFFSET);
                return ret;
            case 'd':
                Column ret2 = new Column();
                ret2.name = name;
                ret2.type = ColumnType.DOUBLE;
                ret2.value = new Double(Double.parseDouble(data.substring(1)) - INT_OFFSET);
                return ret2;
            }

            // unrecognized type char
            return null;
        }
    }
    
    public double getLastOperationCost() {
    	return lastCost;
    }
}
