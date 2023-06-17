package cn.edu.thssdb.schema;

import cn.edu.thssdb.index.BPlusTree;
import cn.edu.thssdb.type.ColumnType;
import cn.edu.thssdb.utils.Pair;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class View implements Iterable<Row> {

  ReentrantReadWriteLock lock;
  public String tableName;
  public ArrayList<Column> columns;
  public BPlusTree<Entry, Row> index;
  // 获得一个oldTable中满足condition的子table
  public View(Table oldTable, String columnName, Comparable rightValue) {
    this.tableName = oldTable.tableName;
    this.columns = oldTable.columns;
    this.index = new BPlusTree<>();
    int primaryIndex = oldTable.GetPrimaryIndex();
    System.out.println("columnName: " + columnName + " rightValue: " + rightValue);
    int i = 0;
    for (Column column : columns) {
      if (column.getName().equals(columnName)) {
        // 从oldTable中筛选出满足condition的行
        ColumnType type = column.getType();
        for (Row row : oldTable) {
          // 判断是否符合condition，符合则insert
          if (type == ColumnType.INT
              || type == ColumnType.LONG
              || type == ColumnType.FLOAT
              || type == ColumnType.DOUBLE) {
            if (rightValue.compareTo(Double.parseDouble(row.getEntries().get(i).toString())) == 0) {
              index.put(row.getEntries().get(primaryIndex), row);
            }
          }
          // todo 更多类型判断
          else {
            if (rightValue.compareTo(row.getEntries().get(i)) == 0) {
              index.put(row.getEntries().get(primaryIndex), row);
            }
          }
        }
        break;
      }
      i++;
    }
    // 从oldTable中筛选出满足condition的行
    for (Row row : oldTable) {
      // 判断是否符合condition，符合则insert

    }
  }

  private class ViewIterator implements Iterator<Row> {
    private Iterator<Pair<Entry, Row>> iterator;

    ViewIterator(View view) {
      this.iterator = index.iterator();
    }

    @Override
    public boolean hasNext() {
      return iterator.hasNext();
    }

    @Override
    public Row next() {
      return iterator.next().right;
    }
  }

  @Override
  public Iterator<Row> iterator() {
    return new View.ViewIterator(this);
  }
}
