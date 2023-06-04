package cn.edu.thssdb.schema;

import cn.edu.thssdb.exception.DuplicateKeyException;
import cn.edu.thssdb.exception.KeyNotExistException;
import cn.edu.thssdb.exception.NullValueException;
import cn.edu.thssdb.query.*;
import cn.edu.thssdb.storage.Cache;
import cn.edu.thssdb.type.ColumnType;
import cn.edu.thssdb.utils.Pair;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static cn.edu.thssdb.utils.Global.storage_dir;

public class Table implements Iterable<Row> {
  ReentrantReadWriteLock lock;
  private String databaseName;
  public String tableName;
  public ArrayList<Column> columns;
  private int primaryIndex; // 主键的索引 就是主键在columns中的下标
  public Cache cache;

  int tplock = 0;
  public ArrayList<Long> s_lock_list;
  public ArrayList<Long> x_lock_list;

  public Table(String databaseName, String tableName, Column[] columns) {
    this.lock = new ReentrantReadWriteLock();
    this.databaseName = databaseName;
    this.tableName = tableName;
    this.columns = new ArrayList<>();
    this.primaryIndex = -1;
    for (int i = 0; i < columns.length; i++) {
      this.columns.add(columns[i]);
      if (columns[i].getPrimary() == 1) {
        this.primaryIndex = i;
      }
    }
    if (primaryIndex < 0 || primaryIndex >= this.columns.size()) {
      throw new RuntimeException("PrimaryNotExistException" + tableName);
    }
    this.cache = new Cache(databaseName, tableName);
    this.s_lock_list = new ArrayList<>();
    this.x_lock_list = new ArrayList<>();
    this.tplock = 0;
    recover();
  }
  // get_s_lock函数根据session获取s锁
  public int get_s_lock(long session) {
    int value = 0; // 返回-1代表加锁失败  返回0代表成功但未加锁  返回1代表成功加锁
    if (tplock == 2) {
      if (x_lock_list.contains(session)) { // 自身已经有更高级的锁了 用x锁去读，未加锁
        value = 0;
      } else {
        value = -1; // 别的session占用x锁，未加锁
      }
    } else if (tplock == 1) {
      if (s_lock_list.contains(session)) { // 自身已经有s锁了 用s锁去读，未加锁
        value = 0;
      } else {
        s_lock_list.add(session); // 其他session加了s锁 把自己加上
        tplock = 1;
        value = 1;
      }
    } else if (tplock == 0) {
      s_lock_list.add(session); // 未加锁 把自己加上
      tplock = 1;
      value = 1;
    }
    return value;
  }

  public int get_x_lock(long session) {
    int value = 0; // 返回-1代表加锁失败  返回0代表成功但未加锁  返回1代表成功加锁
    if (tplock == 2) {
      if (x_lock_list.contains(session)) { // 自身已经取得x锁
        value = 0;
      } else {
        value = -1; // 获取x锁失败
      }
    } else if (tplock == 1) {
      value = -1; // 正在被其他s锁占用
    } else if (tplock == 0) {
      x_lock_list.add(session);
      tplock = 2;
      value = 1;
    }
    return value;
  }

  public void free_s_lock(long session) {
    if (s_lock_list.contains(session)) {
      s_lock_list.remove(session);
      if (s_lock_list.size() == 0) {
        tplock = 0;
      } else {
        tplock = 1;
      }
    }
  }

  public void free_x_lock(long session) {
    if (x_lock_list.contains(session)) {
      tplock = 0;
      x_lock_list.remove(session);
    }
  }

  // getRow函数根据主键的值获取行数据
  public Row getRow(Entry primary_emrty) {
    Row row;
    try {
      lock.readLock().lock();
      row = cache.getRow(primary_emrty, primaryIndex);
    } catch (KeyNotExistException e) {
      throw e;
    } finally {
      lock.readLock().unlock();
    }
    return row;
  }
  // getprimaryname函数获取主键的名字
  public String GetPrimaryName() {
    if (this.primaryIndex < 0 || this.primaryIndex >= this.columns.size()) {
      return null;
    }
    return this.columns.get(this.primaryIndex).getName();
  }
  // getprimaryindex函数获取主键的下标
  public int GetPrimaryIndex() {
    return this.primaryIndex;
  }

  // recover函数从页式文件中读取table的行数据，恢复到内存中
  private void recover() {
    // 获取存储目录下的所有文件
    File dir = new File(storage_dir);
    if (!dir.exists() || !dir.isDirectory()) return;
    File[] fileList = dir.listFiles();
    if (fileList == null) return;
    // 遍历所有文件，找到属于该表的文件
    HashMap<Integer, File> pageFileList = new HashMap<>();
    int pageNum = 0; // 记录最大的页号
    // 正则表达式匹配文件名
    String regex = "page_" + databaseName + "_" + tableName + "_\\d+\\.data";
    Pattern pattern = Pattern.compile(regex);
    for (File f : fileList) {
      if (f != null && f.isFile()) {
        Matcher matcher = pattern.matcher(f.getName());
        if (!matcher.matches()) continue;
        String[] parts = f.getName().split("\\.")[0].split("_");
        int id = Integer.parseInt(parts[3]);
        pageFileList.put(id, f);
        if (id > pageNum) pageNum = id;
      }
    }
    // 从文件中读取数据 需要注意页号是从1开始的
    for (int i = 1; i <= pageNum; i++) {
      File f = pageFileList.get(i);
      ArrayList<Row> rows = deserialize(f);
      cache.insertPage(rows, primaryIndex);
    }
  }

  // show函数将table的元数据都展示出来
  public String show() {
    String ret = "------------------------------------------------------\n";
    for (Column column : columns) {
      ret += column.show() + "\n";
    }
    ret += "------------------------------------------------------";
    return ret;
  }

  /**
   * 描述：将comparer类型的value转换成column的类型 参数：column，value 返回：新的值--comparable，如果不匹配会抛出异常
   * 如果value是null,判断是否符合可空规则 如果value是column，报错 如果column是数字，value是string，或者相反，报错
   * 如果column和value都是数字，value强制类型转换成column类型，比如你把id：int改成5.01，那变成5
   */
  private Comparable ParseValue(Column the_column, Comparer value) {
    if (value == null || value.mValue == null || value.mType == ComparerType.NULL) {
      if (the_column.NotNull()) {
        throw new NullValueException(the_column.getName());
      } else {
        return null;
      }
    }
    String string_value = value.mValue + "";
    if (value.mType == ComparerType.COLUMN) {
      if (the_column.getType().equals(ColumnType.STRING)) {
        throw new RuntimeException(
            "TypeNotMatchException:" + ComparerType.COLUMN + " " + ComparerType.STRING);
      } else {
        throw new RuntimeException(
            "TypeNotMatchException" + ComparerType.COLUMN + " " + ComparerType.NUMBER);
      }
    }

    switch (the_column.getType()) {
      case DOUBLE:
        if (value.mType == ComparerType.STRING) {
          throw new RuntimeException(
              "TypeNotMatchException:" + ComparerType.STRING + " " + ComparerType.NUMBER);
        }
        return Double.parseDouble(string_value);
      case INT:
        if (value.mType == ComparerType.STRING) {
          throw new RuntimeException(
              "TypeNotMatchException:" + ComparerType.STRING + " " + ComparerType.NUMBER);
        }
        double double_value = Double.parseDouble(string_value);
        int int_value = (int) double_value;
        return Integer.parseInt(int_value + "");
      case FLOAT:
        if (value.mType == ComparerType.STRING) {
          throw new RuntimeException(
              "TypeNotMatchException:" + ComparerType.STRING + " " + ComparerType.NUMBER);
        }
        return Float.parseFloat(string_value);
      case LONG:
        if (value.mType == ComparerType.STRING) {
          throw new RuntimeException(
              "TypeNotMatchException:" + ComparerType.STRING + " " + ComparerType.NUMBER);
        }
        double double_value_2 = Double.parseDouble(string_value);
        long long_value = (long) double_value_2;
        return Long.parseLong(long_value + "");
      case STRING:
        if (value.mType == ComparerType.NUMBER) {
          throw new RuntimeException(
              "TypeNotMatchException:" + ComparerType.STRING + " " + ComparerType.NUMBER);
        }
        return string_value;
    }
    return null;
  }
  // ParseValue函数的重载，将string类型的value转换成column的类型
  private Comparable ParseValue(Column the_column, String value) {
    if (value.equals("null")) {
      if (the_column.NotNull()) {
        throw new NullValueException(the_column.getName());
      } else {
        return null;
      }
    }
    switch (the_column.getType()) {
      case DOUBLE:
        return Double.parseDouble(value);
      case INT:
        return Integer.parseInt(value);
      case FLOAT:
        return Float.parseFloat(value);
      case LONG:
        return Long.parseLong(value);
      case STRING:
        return value.substring(1, value.length() - 1);
    }
    return null;
  }

  private void JudgeValid(Column the_column, Comparable new_value) { // 这里使用Comparable是因为可以接受多种格式的数据
    boolean not_null = the_column.NotNull();
    ColumnType the_type = the_column.getType();
    int max_length = the_column.getMaxLength();
    if (not_null == true && new_value == null) {
      throw new NullValueException(the_column.getName());
    }
    if (the_type == ColumnType.STRING && new_value != null) {
      if (max_length >= 0 && (new_value + "").length() > max_length) {
        throw new RuntimeException("ValueLengthExceedException:" + the_column.getName());
      }
    }
  }

  public String update(String column_name, Comparer value, Logic the_logic) {
    int count = 0;
    boolean whether_find = false;
    Column the_column = null;
    for (Column column : this.columns) {
      if (column.getName().equals(column_name)) {
        the_column = column;
        whether_find = true;
        break;
      }
    }
    if (the_column == null || whether_find == false) {
      throw new RuntimeException("AttributeNotFoundException:" + column_name);
    }
    for (Row row : this) { // this是table的迭代器,row是table的行
      JointRow the_row = new JointRow(row, this);
      if (the_logic == null || the_logic.GetResult(the_row) == ResultType.TRUE) {
        Entry primary_entry = row.getEntries().get(primaryIndex);
        // 值处理，合法性判断
        Comparable the_entry_value = ParseValue(the_column, value);

        JudgeValid(the_column, the_entry_value);

        // 插入
        Entry the_entry = new Entry(the_entry_value);
        ArrayList<Column> the_column_list = new ArrayList<>();
        the_column_list.add(the_column);
        ArrayList<Entry> the_entry_list = new ArrayList<>();
        the_entry_list.add(the_entry);
        update(primary_entry, the_column_list, the_entry_list, false);
        count++;
      }
    }
    return "Updated " + count + " items.";
  }

  // update函数更新主键值所对应的行
  public void update(
      Entry primary_entry, ArrayList<Column> columns, ArrayList<Entry> entries, boolean in_tran) {
    if (primary_entry == null || columns == null || entries == null)
      throw new KeyNotExistException(null);

    HashMap<Column, Integer> columnMap = new HashMap<>();
    int tableColumnSize = this.columns.size();
    // 构建列到索引的映射
    for (int j = 0; j < tableColumnSize; j++) {
      columnMap.put(this.columns.get(j), j);
    }
    int targetKeys[] = new int[columns.size()];
    for (int i = 0; i < columns.size(); i++) {
      Column column = columns.get(i);
      Integer columnIndex = columnMap.get(column);
      targetKeys[i] = columnIndex;
    }

    try {
      lock.writeLock().lock();
      cache.updateRow(primary_entry, primaryIndex, targetKeys, entries, in_tran);
    } catch (KeyNotExistException | DuplicateKeyException e) {
      throw e;
    } finally {
      lock.writeLock().unlock();
    }
  }

  // 这个insert插入的数据是自带顺序的，所以不需要重新排序，转化为Entry然后插入即可
  public void insert(String[] value_list, boolean in_tran) {
    if (value_list == null) return;
    int len = this.columns.size(); // 当前表的列数

    if (value_list.length != len) {
      throw new RuntimeException("insert value list length not match column length");
    }
    ArrayList<Entry> orderedEntries = new ArrayList<>();
    for (int i = 0; i < this.columns.size(); i++) {
      Column column = this.columns.get(i);
      Comparable the_entry_value = ParseValue(column, value_list[i]);
      JudgeValid(column, the_entry_value);
      orderedEntries.add(new Entry(the_entry_value));
    }
    // write to cache
    try {
      lock.writeLock().lock();
      cache.insertRow(orderedEntries, primaryIndex, in_tran);
    } catch (DuplicateKeyException e) {
      throw e;
    } finally {
      lock.writeLock().unlock();
    }
  }

  //   insert函数将传入的列数据按照当前table的列顺序插入到cache中
  public void insert(String[] column_list, String[] value_list, boolean in_tran) {
    if (column_list == null || value_list == null) return;
    int len = this.columns.size(); // 当前表的列数
    if (column_list.length != len || value_list.length != len)
      throw new RuntimeException("column_list.length != len || value_list.length != len");

    // 将entries按照当前table列的顺序排序
    ArrayList<Entry> entry_list_sorted = new ArrayList<>();
    HashMap<String, Integer> column_index = new HashMap<>();
    for (int i = 0; i < len; i++) {
      if (column_index.get(column_list[i]) != null)
        throw new RuntimeException("column_list中存在重复的列名");
      column_index.put(column_list[i], i);
    }
    for (Column column : this.columns) {
      Integer index = column_index.get(column.getName());
      Comparable the_entry_value = null;
      if (index != null) {
        the_entry_value = ParseValue(column, value_list[index]);
      } else {
        the_entry_value = ParseValue(column, "null");
      }
      JudgeValid(column, the_entry_value);
      entry_list_sorted.add(new Entry(the_entry_value));
    }
    // 将这个entry_list_sorted插入到cache中
    try {
      lock.writeLock().lock();
      cache.insertRow(entry_list_sorted, primaryIndex, in_tran);
    } catch (DuplicateKeyException e) {
      throw e;
    } finally {
      lock.writeLock().unlock();
    }
  }

  // delete函数删除主键值所对应的行
  public void delete(Entry primary_entry, boolean in_tran) {
    if (primary_entry == null) return;
    try {
      lock.writeLock().lock();
      cache.deleteRow(primary_entry, primaryIndex, in_tran);
    } finally {
      lock.writeLock().unlock();
    }
  }

  public String delete(Logic the_logic) {
    int count = 0;
    for (Row row : this) {
      JointRow the_row = new JointRow(row, this);
      if (the_logic == null || the_logic.GetResult(the_row) == ResultType.TRUE) {
        Entry primary_entry = row.getEntries().get(primaryIndex);
        delete(primary_entry, false); // 这里加了一个false，后面修改需要注意一下
        count++;
      }
    }
    return "Deleted " + count + " items.";
  }

  // dropSelf函数删除table的所有数据
  public void dropSelf() {
    try {
      lock.writeLock().lock();
      cache.drop_all();
      cache = null;

      File dir = new File(storage_dir);
      File[] fileList = dir.listFiles();
      if (fileList == null) return;
      // 这个正则表达式需要在page_database_table_pageid.data和meta_database_table.data这两种文件中匹配database_table符合要求的文件
      String regex = "^(page|meta)_" + databaseName + "_" + tableName + "(_\\d+)?\\.data$";
      Pattern pattern = Pattern.compile(regex);
      for (File f : fileList) {
        if (f != null && f.isFile()) {
          Matcher matcher = pattern.matcher(f.getName());
          if (matcher.matches()) {
            f.delete();
          }
        }
      }
      columns.clear();
      columns = null;
    } finally {
      lock.writeLock().unlock();
    }
  }

  // deserialize函数将文件反序列化为ArrayList<Row>,这里通过调用
  private ArrayList<Row> deserialize(File file) {
    return cache.data_deserialize(file);
  }

  // persist函数将cache中的数据持久化到文件中
  public void persist() {
    try {
      lock.writeLock().lock();
      cache.data_persist();
    } finally {
      lock.writeLock().unlock();
    }
  }

  // 解除事务
  public void quit_tran() {
    cache.quit_tran();
  }

  private class TableIterator implements Iterator<Row> {
    private Iterator<Pair<Entry, Row>> iterator;

    TableIterator(Table table) {
      this.iterator = table.cache.getIndexIter();
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
    return new TableIterator(this);
  }
}
