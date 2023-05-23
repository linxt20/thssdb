package cn.edu.thssdb.plan.impl;

import cn.edu.thssdb.plan.LogicalPlan;
import cn.edu.thssdb.query.Logic;

import java.util.ArrayList;

public class SelectPlan extends LogicalPlan {

  private String[] columns;
  private Logic logicForJoin;
  private Logic logic;
  private Boolean distinct;
  private ArrayList<String> tableNames;

  public SelectPlan(
      ArrayList<String> tableNames,
      String[] columns,
      Logic logicForJoin,
      Logic logic,
      Boolean distinct) {
    super(LogicalPlanType.SELECT);
    this.tableNames = tableNames;
    this.columns = columns;
    this.logicForJoin = logicForJoin;
    this.logic = logic;
    this.distinct = distinct;
  }

  public ArrayList<String> getTableNames() {
    return tableNames;
  }

  public String[] getColumns() {
    return columns;
  }

  public Logic getLogicForJoin() {
    return logicForJoin;
  }

  public Logic getLogic() {
    return logic;
  }

  public Boolean getDistinct() {
    return distinct;
  }

  @Override
  public String toString() {
    String columnsString = "";
    for (String column : columns) {
      columnsString = column + " ";
    }
    return "SelectPlan{" + "columns='" + columnsString + '\'' + '}';
  }
}
