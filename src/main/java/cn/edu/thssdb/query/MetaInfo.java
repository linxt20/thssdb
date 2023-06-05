package cn.edu.thssdb.query;

import cn.edu.thssdb.schema.Column;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

class MetaInfo {

  private String tableName;
  public List<Column> columns;
  private HashMap<String, Integer> columnIndex;

  MetaInfo(String tableName, ArrayList<Column> columns) {
    this.tableName = tableName;
    this.columns = columns;
    this.columnIndex = new HashMap<>();
    for (int i = 0; i < columns.size(); i++) {
      columnIndex.put(columns.get(i).getName(), i);
    }
  }

  /** 描述：找到对应列的位置 参数：列名 返回：位置i */
  int ColumnFind(String name) {
    Integer index = columnIndex.get(name);
    return index != null ? index : -1;
  }

  /** 描述：返回对应列全名 参数：列index 返回：全名，tablename.attrname */
  String GetFullName(int index) {
    if (index < 0 || index >= columns.size()) {
      return null;
    }
    String name = tableName + "." + columns.get(index).getName();
    return name;
  }

  int GetColumnSize() {
    return columns.size();
  }

  String GetTableName() {
    return tableName;
  }
}
