package cn.edu.thssdb.plan.impl;

import cn.edu.thssdb.plan.LogicalPlan;
import cn.edu.thssdb.sql.SQLParser;

import java.util.List;

public class InsertPlan extends LogicalPlan {
  private final String tableName;

  private final String[] columnNames;

  private final List<SQLParser.ValueEntryContext> valueEntryContextList;

  public InsertPlan(
      String tableName,
      String[] columnNames,
      List<SQLParser.ValueEntryContext> valueEntryContextList) {
    super(LogicalPlanType.INSERT);
    System.out.println("InsertPlan: [DEBUG] " + tableName);
    this.tableName = tableName;
    this.columnNames = columnNames;
    this.valueEntryContextList = valueEntryContextList;
  }

  public String getTableName() {
    return tableName;
  }

  public List<SQLParser.ValueEntryContext> getValueEntryContextList() {
    return valueEntryContextList;
  }

  public String[] getColumnNames() {
    return columnNames;
  }
}
