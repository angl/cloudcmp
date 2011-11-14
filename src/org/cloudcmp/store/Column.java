package org.cloudcmp.store;

import java.lang.reflect.Type;
import java.util.*;

public class Column { // for now we only support string columns
    public String name;
    public Object value;
    public ColumnType type;
    public boolean isID = false; // whether the column corresponds to the id. used only in condition

    public Column() {}
    public Column(String n, String v) {
        name = n;
        value = v;
        type = ColumnType.STRING;
    }

    public Column(String n, int v) {
        name = n;
        value = new Integer(v);
        type = ColumnType.INT;
    }

    public Column(String n, double v) {
        name = n;
        value = new Double(v);
        type = ColumnType.DOUBLE;
    }
}


