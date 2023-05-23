package cn.edu.thssdb.plan.impl;

import cn.edu.thssdb.plan.LogicalPlan;
import cn.edu.thssdb.query.Logic;

public class DeletePlan extends LogicalPlan {
  private String tableName;
  private Logic condition;

  public DeletePlan(String tableName, Logic condition) {
    super(LogicalPlanType.DELETE);
    this.tableName = tableName;
    this.condition = condition;
  }

  public String getTableName() {
    return tableName;
  }

  public Logic getCondition() {
    return condition;
  }

  @Override
  public String toString() {
    return "DeletePlan{"
        + "tableName='"
        + tableName
        + '\''
        + ", condition='"
        + condition
        + '\''
        + '}';
  }
}
