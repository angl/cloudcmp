package org.cloudcmp.store;
import java.util.*;

//a helper class to model a table's schema. useful to generate rows
//for the same table
public class TableSchema {
	List<String> columnNames = new ArrayList<String>();
	List<ColumnType> columnTypes = new ArrayList<ColumnType>();

	public Set<Integer> idColumns = new TreeSet<Integer>(); // which column(s) is(are) the id
	public String tableName = "";

	public int getColumnCount() {
		return columnNames.size();
	}

	public String getColumnName(int i) {
		return columnNames.get(i);
	}

	public ColumnType getColumnType(int i) {
		return columnTypes.get(i);
	}

	public void addColumn(String columnName, ColumnType columnType, boolean isId) {
		columnNames.add(columnName);
		columnTypes.add(columnType);
		if (isId) idColumns.add(new Integer(columnNames.size() - 1));
	}

	public TableSchema() { }
	public TableSchema(String tableLine) {
		// decode the table definition line and generate the schema
		String delimiter = " ";
		if (tableLine.contains("\t")) delimiter = "\t";

		String [] parts = tableLine.split(delimiter);
		if (parts.length == 0) return;

		tableName = parts[0];

		for (int i = 1; i < parts.length; ++ i) {
			// column name and type are separated by ":"
			if (parts[i].indexOf(':') < 0) continue;
			String columnName = parts[i].substring(0, parts[i].indexOf(':'));
			String columnType = parts[i].substring(parts[i].indexOf(':') + 1);
			boolean isId = false;

			if (columnType.contains(":")) {
				if (columnType.substring(columnType.indexOf(':') + 1).equals("key")) {
					isId = true;
					columnType = columnType.substring(0, columnType.indexOf(':'));
				}
			}

			if (columnName.equals("") || columnType.equals("")) continue;

			ColumnType realColumnType = null;
			if (columnType.equals("STRING")) realColumnType = ColumnType.STRING;
			else if (columnType.equals("INT")) realColumnType = ColumnType.INT;
			else if (columnType.equals("DOUBLE")) realColumnType = ColumnType.DOUBLE;

			if (realColumnType == null) continue;

			columnNames.add(columnName);
			columnTypes.add(realColumnType);
			if (isId) idColumns.add(new Integer(columnNames.size() - 1));
		}
	}

	public Row getRow(String dataLine) {
		// generate a row from the schema and a line of data (compatible with sql dump)
		String delimiter = " ";
		if (dataLine.contains("\t")) delimiter = "\t";

		String [] parts = dataLine.split(delimiter);
		if (parts.length == 0 || parts.length != columnNames.size()) return null;

		// trim the leading and trailing spaces...
		for (int i = 0; i < parts.length; ++ i) {
			parts[i] = parts[i].trim();
		}

		// if there is only one key, make the key column the id
		// otherwise, make a new key by concatenating the values in all key columns

		String key = "";
		for (Integer idColumn : idColumns) {
			String id = parts[idColumn.intValue()];
			if (key.equals(""))
				key = key + id;
			else
				key = key + "-" + id;
		}

		Row row = new Row(key);
		for (int i = 0; i < parts.length; ++ i) {
			if (idColumns.contains(new Integer(i)) && idColumns.size() == 1) continue;

			Column column = new Column();
			column.name = columnNames.get(i);
			column.type = columnTypes.get(i);

			switch (column.type) {
			case STRING:
				column.value = parts[i]; break;
			case INT:
				column.value = Integer.parseInt(parts[i]); break;
			case DOUBLE:
				column.value = Double.parseDouble(parts[i]); break;
			}

			row.columns.put(column.name, column);
		}

		return row;
	}
}
