package cn.edu.thssdb.plan.impl;

import cn.edu.thssdb.plan.LogicalPlan;
import cn.edu.thssdb.query.Logic;

import java.util.ArrayList;

public class SelectPlan extends LogicalPlan {

  private String[] columns;
  private int joinType;
  private Logic logic, whereLogic;
  private Boolean distinct;
  private ArrayList<String> tableNames;

  public SelectPlan(
      ArrayList<String> tableNames,
      String[] columns,
      Logic logic,
      Logic whereLogic,
      Boolean distinct,
      int joinType) {
    super(LogicalPlanType.SELECT);
    this.tableNames = tableNames;
    this.columns = columns;
    this.logic = logic;
    this.whereLogic = whereLogic;
    this.distinct = distinct;
    this.joinType = joinType;
  }

  public SelectPlan(
          ArrayList<String> tableNames,
          String[] columns,
          Logic logic,
          Boolean distinct,
          int joinType) {
    super(LogicalPlanType.SELECT);
    this.tableNames = tableNames;
    this.columns = columns;
    this.logic = logic;
    this.whereLogic = null;
    this.distinct = distinct;
    this.joinType = joinType;
  }

  public Logic getWhereLogic() {
    return whereLogic;
  }

  public ArrayList<String> getTableNames() {
    return tableNames;
  }

  public String[] getColumns() {
    return columns;
  }

  public int getJoinType() {
    return joinType;
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
