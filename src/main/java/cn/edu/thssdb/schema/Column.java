package cn.edu.thssdb.schema;

import cn.edu.thssdb.type.ColumnType;

public class Column implements Comparable<Column> {
  private String name;
  private ColumnType type;
  private int primary;
  private boolean notNull;
  private int maxLength;

  public Column(String name, ColumnType type, int primary, boolean notNull, int maxLength) {
    this.name = name;
    this.type = type;
    this.primary = primary;
    this.notNull = notNull;
    this.maxLength = maxLength;
  }

  public String getName() {
    return this.name;
  }

  public ColumnType getType() {
    return this.type;
  }

  public void setPrimary(int new_primary) {
    primary = new_primary;
    notNull = true;
  }

  public int getPrimary() {
    return primary;
  }

  public boolean NotNull() {
    return this.notNull;
  }

  public int getMaxLength() {
    return maxLength;
  }

  @Override
  public int compareTo(Column e) {
    return name.compareTo(e.name);
  }

  public String toString() {
    return name + ',' + type + ',' + primary + ',' + notNull + ',' + maxLength;
  }

  public String show() {
    String ret = name + "\t" + type;
    if (type.equals(ColumnType.STRING)) {
      ret += "(" + maxLength + ")";
    }
    if (primary == 1) {
      ret += "\tprimary key";
    }
    if (notNull) {
      ret += "\tnot null";
    }
    return ret;
  }
}
