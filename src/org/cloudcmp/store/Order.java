package org.cloudcmp.store;

public class Order {
    public String columnName;
    public OrderType type;

    public Order() {}
    public Order(String orderName, OrderType orderType) {
        columnName = orderName;
        type = orderType;
    }
}
