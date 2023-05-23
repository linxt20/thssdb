package cn.edu.thssdb.plan.impl;

import cn.edu.thssdb.plan.LogicalPlan;
import cn.edu.thssdb.query.Comparer;
import cn.edu.thssdb.query.Logic;

public class UpdatePlan extends LogicalPlan {

  String tableName;
  String columnName;
  Comparer value;
  Logic logic;

  public UpdatePlan(String tableName, String columnName, Comparer value, Logic logic) {
    super(LogicalPlanType.UPDATE);
    this.tableName = tableName;
    this.columnName = columnName;
    this.value = value;
    this.logic = logic;
  }

  public String getTableName() {
    return tableName;
  }

  public String getColumnName() {
    return columnName;
  }

  public Comparer getValue() {
    return value;
  }

  public Logic getLogic() {
    return logic;
  }

  @Override
  public String toString() {
    return "UpdatePlan{" + "tableName='" + '\'' + '}';
  }
}
