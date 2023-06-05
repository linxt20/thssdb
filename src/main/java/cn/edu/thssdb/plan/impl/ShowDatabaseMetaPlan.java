package cn.edu.thssdb.plan.impl;

import cn.edu.thssdb.plan.LogicalPlan;

public class ShowDatabaseMetaPlan extends LogicalPlan {
  private final String databaseName;

  public ShowDatabaseMetaPlan(String databaseName) {
    super(LogicalPlanType.SHOW_DATABASE_META);
    this.databaseName = databaseName;
  }

  public String getDatabaseName() {
    return databaseName;
  }

  @Override
  public String toString() {
    return "ShowDatabaseMetaPlan{" + "databaseName='" + databaseName + '\'' + "}";
  }
}
