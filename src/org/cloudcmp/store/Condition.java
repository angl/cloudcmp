package org.cloudcmp.store;

public class Condition {
    public Column target;
    public ConditionType type;

    public Condition() {}
    public Condition(String columnName, ConditionType conditionType, ColumnType columnType, Object columnValue) {
        target = new Column();
        target.name = columnName;
        target.type = columnType;
        target.value = columnValue;
        type = conditionType;
        target.isID = false;
    }

    // on id
    public Condition(ConditionType conditionType, ColumnType columnType, Object columnValue) {
        target = new Column();
        target.name = "";
        target.type = columnType;
        target.value = columnValue;
        type = conditionType;
        target.isID = true;
    }
}
