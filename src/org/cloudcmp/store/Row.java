package org.cloudcmp.store;

import java.util.*;
import java.text.DateFormat;
import java.text.SimpleDateFormat;

// also make the Rows comparable
public class Row implements Comparable {
	public String id;
	public TreeMap<String, Column> columns;
	public String compareColumn = ""; // the column to compare

	public void setCompareColumn(String columnName) { // if we want to sort the rows
		compareColumn = columnName;
	}

	public int compareTo(Object o) {
		Row r = (Row)o;
		if (r.compareColumn.equals("") || !compareColumn.equals(r.compareColumn)) return 0;
		Column c1 = columns.get(compareColumn);
		Column c2 = r.columns.get(r.compareColumn);

		switch (c1.type) {
		case STRING:
			return ((String)c1.value).compareTo((String)c2.value);
		case INT:
			return ((Integer)c1.value).compareTo((Integer)c2.value);
		case DOUBLE:
			return ((Double)c1.value).compareTo((Double)c2.value);
		}

		return 0;
	}

	public Row() {
		id = UUID.randomUUID().toString();
		columns = new TreeMap<String, Column>();
	}

	public Row(String _id) {
		id = _id;
		columns = new TreeMap<String, Column>();
	}

	public Column getColumn(String columnName) {
		return columns.get(columnName);
	}

	public void setColumn(Column column) {
		columns.put(column.name, column);
	}

	public String getString(String columnName) {
		return (String)(columns.get(columnName).value);
	}

	public int getInt(String columnName) {
		return ((Integer)(columns.get(columnName).value)).intValue();
	}

	public double getDouble(String columnName) {
		return ((Double)(columns.get(columnName).value)).doubleValue();
	}
	// note we do not support Date as a base type. the string needs to be parsed by DateFormat
	public Date getDate(String columnName, String format) {
		DateFormat f = new SimpleDateFormat(format);
		try {
			return f.parse((String)(columns.get(columnName).value));
		}
		catch (Exception ex) {
			return null;
		}
	}

	public void setString(String columnName, String value) {
		if (!columns.containsKey(columnName)) {
			Column column = new Column(columnName, value);
			columns.put(columnName, column);
		}
		else {
			columns.get(columnName).value = value;
		}
	}

	public void setInt(String columnName, int value) {
		if (!columns.containsKey(columnName)) {
			Column column = new Column(columnName, value);
			columns.put(columnName, column);
		}
		else {
			columns.get(columnName).value = new Integer(value);
		}
	}
	public void setDouble(String columnName, double value) {
		if (!columns.containsKey(columnName)) {
			Column column = new Column(columnName, value);
			columns.put(columnName, column);
		}
		else {
			columns.get(columnName).value = new Double(value);
		}
	}
}
