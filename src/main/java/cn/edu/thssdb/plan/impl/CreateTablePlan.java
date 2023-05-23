package cn.edu.thssdb.plan.impl;

import cn.edu.thssdb.plan.LogicalPlan;
import cn.edu.thssdb.schema.Column;

public class CreateTablePlan extends LogicalPlan {

  private String tableName;
  private Column[] columns;

  public CreateTablePlan(String tableName, Column[] columns) {
    super(LogicalPlanType.CREATE_TABLE);
    this.tableName = tableName;
    this.columns = columns;
  }

  public String getTableName() {
    return tableName;
  }

  public Column[] getColumns() {
    return columns;
  }

  @Override
  public String toString() {
    return "CreateTablePlan{" + "tableName='" + tableName + '\'' + '}';
  }
}
