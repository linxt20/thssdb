package cn.edu.thssdb.plan.impl;

import cn.edu.thssdb.item.TypeItem;
import cn.edu.thssdb.plan.LogicalPlan;
import cn.edu.thssdb.schema.Table;
import cn.edu.thssdb.type.ColumnType;

public class AlterTablePlan extends LogicalPlan {
  private String tableName; // 表名称
  private String columnName;

  private String columnNewName;
  private String opType;//add 添加，drop 删除，alter 修改，change 换名
  private ColumnType columnType;

  private int maxLength;//对string为最大长度，其他为-1
  private Table table;


  /** [method] 构造方法 */
  public AlterTablePlan(String tableName, String columnName, String opType, ColumnType columnType, int maxLength) {
    super(LogicalPlanType.ALTER_TABLE);
    this.tableName = tableName;
    this.columnName = columnName;
    this.opType = opType;
    this.columnType = columnType;
    this.maxLength = maxLength;
  }

  public ColumnType getColumnType() {
    return columnType;
  }

  public int getMaxLength() {
    return maxLength;
  }

  public String getColumnName() {
    return columnName;
  }

  public String getColumnNewName() {
    return columnNewName;
  }

  public String getOpType() {
    return opType;
  }

  public String getTableName() {
    return tableName;
  }


  public Table getTable() {
    return table;
  }

  /** [method] 执行操作 */
}
