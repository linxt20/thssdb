package cn.edu.thssdb.schema;

import cn.edu.thssdb.exception.DuplicateKeyException;
import cn.edu.thssdb.exception.KeyNotExistException;
import cn.edu.thssdb.exception.NullValueException;
import cn.edu.thssdb.exception.SchemaLengthMismatchException;
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

  // TODO 暂时不考虑锁，后面再补充

  public Table(String databaseName, String tableName, Column[] columns) {
    System.out.println("==========Table constructor=============");
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
    // TODO primaryIndex如果没有被更新需要抛出异常
    this.cache = new Cache(databaseName, tableName);
    // TODO 一些后面要加的变量
    recover();
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

  // 将table中的所有数据清空
  public void dropall() {
    try {
      lock.writeLock().lock();
      cache.drop_all();
      cache = null;

      File dir = new File(storage_dir);
      File[] fileList = dir.listFiles();
      if (fileList == null) return;
      for (File f : fileList) {
        if (f != null && f.isFile()) {
          try {
            String[] parts = f.getName().split("\\.")[0].split("_");
            String databaseName = parts[0];
            String tableName = parts[1];
            if (!(this.databaseName.equals(databaseName) && this.tableName.equals(tableName)))
              continue;
          } catch (Exception e) {
            continue;
          }
          f.delete();
        }
      }

      columns.clear();
      columns = null;
    } finally {
      lock.writeLock().unlock();
    }
  }

  /** 描述：获得主键名称 参数：无 返回：主键名称，如果没有就null */
  public String GetPrimaryName() {
    if (this.primaryIndex < 0 || this.primaryIndex >= this.columns.size()) {
      return null;
    }
    return this.columns.get(this.primaryIndex).getName();
  }

  public Row get(Entry entry) {
    if (entry == null) throw new KeyNotExistException(null);

    Row row;
    try {
      lock.readLock().lock();
      row = cache.getRow(entry, primaryIndex);
    } catch (KeyNotExistException e) {
      throw e;
    } finally {
      lock.readLock().unlock();
    }
    return row;
  }

  public int GetPrimaryIndex() {
    return this.primaryIndex;
  }

  /** 描述：用于sql parser的插入函数 参数：value数组，string形式 返回：无，如果不合法会抛出异常 */
  public void insert(String[] values) {
    if (values == null) throw new SchemaLengthMismatchException(this.columns.size(), 0);

    // match columns and reorder entries
    int schemaLen = this.columns.size();
    if (values.length > schemaLen) {
      throw new SchemaLengthMismatchException(schemaLen, values.length);
    }

    ArrayList<Entry> orderedEntries = new ArrayList<>();
    for (int i = 0; i < this.columns.size(); i++) {
      Column column = this.columns.get(i);
      Comparable the_entry_value = null;
      if (i >= 0 && i < values.length) {
        the_entry_value = ParseValue(column, values[i]);
      }
      JudgeValid(column, the_entry_value);
      Entry the_entry = new Entry(the_entry_value);
      orderedEntries.add(the_entry);
    }

    // write to cache
    try {
      lock.writeLock().lock();
      cache.insertRow(orderedEntries, primaryIndex, false);
    } catch (DuplicateKeyException e) {
      throw e;
    } finally {
      lock.writeLock().unlock();
    }
  }

  /** 描述：用于sql parser的插入函数 参数：column数组，value数组，都是string形式 返回：无，如果不合法会抛出异常 */
  public void insert(String[] columns, String[] values) {
    if (columns == null || values == null)
      throw new SchemaLengthMismatchException(this.columns.size(), 0);

    // match columns and reorder entries
    int schemaLen = this.columns.size();
    if (columns.length > schemaLen) {
      throw new SchemaLengthMismatchException(schemaLen, columns.length);
    } else if (values.length > schemaLen) {
      throw new SchemaLengthMismatchException(schemaLen, values.length);
    } else if (columns.length != values.length) {
      throw new SchemaLengthMismatchException(columns.length, values.length);
    }
    ArrayList<Entry> orderedEntries = new ArrayList<>();
    for (Column column : this.columns) {
      int equal_num = 0;
      int place = -1;
      for (int i = 0; i < values.length; i++) {
        if (columns[i].equals(column.getName().toLowerCase())) {
          place = i;
          equal_num++;
        }
      }
      if (equal_num > 1) {
        // throw new DuplicateColumnException(column.toString());
      }
      Comparable the_entry_value = null;
      if (equal_num == 0 || place < 0 || place >= columns.length) {
        the_entry_value = null;
      } else {
        the_entry_value = ParseValue(column, values[place]);
      }
      JudgeValid(column, the_entry_value);
      Entry the_entry = new Entry(the_entry_value);
      orderedEntries.add(the_entry);
    }

    // write to cache
    try {
      lock.writeLock().lock();
      cache.insertRow(orderedEntries, primaryIndex, false);
    } catch (DuplicateKeyException e) {
      throw e;
    } finally {
      lock.writeLock().unlock();
    }
  }

  /**
   * 描述：将comparer类型的value转换成column的类型
   * 参数：column，value
   * 返回：新的值--comparable，如果不匹配会抛出异常
   * 如果value是null,判断是否符合可空规则
   * 如果value是column，报错
   * 如果column是数字，value是string，或者相反，报错
   * 如果column和value都是数字，value强制类型转换成column类型，比如你把id：int改成5.01，那变成5
   */
  private Comparable ParseValue(Column the_column, Comparer value) {
    if (value == null || value.mValue == null ||value.mType == ComparerType.NULL) {
      if (the_column.NotNull()) {
        throw new NullValueException(the_column.getName());
      }
      else {
        return null;
      }
    }
    String string_value = value.mValue + "";
    if(value.mType == ComparerType.COLUMN) {
      if(the_column.getType().equals(ColumnType.STRING)) {
        // throw new TypeNotMatchException(ComparerType.COLUMN, ComparerType.STRING);
      }
      else {
        // throw new TypeNotMatchException(ComparerType.COLUMN, ComparerType.NUMBER);
      }
    }

    switch (the_column.getType()) {
      case DOUBLE:
        if(value.mType == ComparerType.STRING) {
          // throw new TypeNotMatchException(ComparerType.STRING, ComparerType.NUMBER);
        }
        return Double.parseDouble(string_value);
      case INT:
        if(value.mType == ComparerType.STRING) {
          // throw new TypeNotMatchException(ComparerType.STRING, ComparerType.NUMBER);
        }
        double double_value = Double.parseDouble(string_value);
        int int_value = (int)double_value;
        return Integer.parseInt(int_value + "");
      case FLOAT:
        if(value.mType == ComparerType.STRING) {
          // throw new TypeNotMatchException(ComparerType.STRING, ComparerType.NUMBER);
        }
        return Float.parseFloat(string_value);
      case LONG:
        if(value.mType == ComparerType.STRING) {
          // throw new TypeNotMatchException(ComparerType.STRING, ComparerType.NUMBER);
        }
        double double_value_2 = Double.parseDouble(string_value);
        long long_value = (long)double_value_2;
        return Long.parseLong(long_value + "");
      case STRING:
        if(value.mType == ComparerType.NUMBER) {
          // throw new TypeNotMatchException(ComparerType.STRING, ComparerType.NUMBER);
        }
        return string_value;
    }
    return null;
  }


  /**
   * 描述：用于sql parser的更新函数
   * 参数：待更新列名，待更新值（string类型），逻辑
   * 返回：字符串，表明更新了多少数据
   */
  public String update(String column_name, Comparer value, Logic the_logic) {
    int count = 0;
    for(Row row : this) {
      JointRow the_row = new JointRow(row, this);
      if(the_logic == null || the_logic.GetResult(the_row) == ResultType.TRUE) {
        Entry primary_entry = row.getEntries().get(primaryIndex);
        //找到对应column
        boolean whether_find = false;
        Column the_column = null;
        for(Column column : this.columns) {
          if(column.getName().equals(column_name)) {
            the_column = column;
            whether_find = true;
            break;
          }
        }
        if(the_column == null || whether_find == false) {
          // throw new AttributeNotFoundException(column_name);
        }

        //值处理，合法性判断
        Comparable the_entry_value = ParseValue(the_column, value);

        JudgeValid(the_column, the_entry_value);

        //插入
        Entry the_entry = new Entry(the_entry_value);
        ArrayList<Column> the_column_list = new ArrayList<>();
        the_column_list.add(the_column);
        ArrayList<Entry> the_entry_list = new ArrayList<>();
        the_entry_list.add(the_entry);
        update(primary_entry, the_column_list, the_entry_list);
        count ++;
      }
    }
    return "Updated " + count + " items.";
  }

  public void update(Entry primaryEntry, ArrayList<Column> columns, ArrayList<Entry> entries) {
//        System.out.println("updatet");
    if (primaryEntry == null || columns == null || entries == null)
      throw new KeyNotExistException(null);

    int targetKeys[] = new int[columns.size()];
    int i = 0;
    int tableColumnSize = this.columns.size();
    for (Column column : columns)
    {
      boolean isMatched = false;
      for (int j = 0; j < tableColumnSize; j++)
      {
        if (column.equals(this.columns.get(j)))
        {
          targetKeys[i] = j;
          isMatched = true;
          break;
        }
      }
      if (!isMatched)
        throw new KeyNotExistException(column.toString());
      i++;
    }

    try {
      lock.writeLock().lock();
      cache.updateRow(primaryEntry, primaryIndex, targetKeys, entries, false);
    }
    catch (KeyNotExistException | DuplicateKeyException e) {
      throw e;
    } finally {
      lock.writeLock().unlock();
    }
  }

  /**
   * 描述：将string类型的value转换成column的类型
   * 参数：column，value
   * 返回：新的值--comparable，如果不匹配会抛出异常
   */
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

  /** 描述：判断value是否合法，符合column规则，这里只判断null和max length 参数：column，value 返回：无，如果不合法会抛出异常 */
  private void JudgeValid(Column the_column, Comparable new_value) {
    boolean not_null = the_column.NotNull();
    ColumnType the_type = the_column.getType();
    int max_length = the_column.getMaxLength();
    if (not_null == true && new_value == null) {
      throw new NullValueException(the_column.getName());
    }
    if (the_type == ColumnType.STRING && new_value != null) {
      if (max_length >= 0 && (new_value + "").length() > max_length) {
        System.out.println("ValueLengthExceedException(the_column.getName())");
        // throw new ValueLengthExceedException(the_column.getName());
      }
    }
  }

  // TODO
  // insert函数将传入的列数据按照当前table的列顺序插入到cache中
  public void insert(ArrayList<Column> columns, ArrayList<Entry> entries, boolean in_tran) {
    if (columns == null || entries == null) return;
    int len = this.columns.size(); // 当前表的列数
    if (columns.size() != len || entries.size() != len) return;
    // 将entries按照当前table列的顺序排序
    ArrayList<Entry> entry_list_sorted = new ArrayList<>();
    HashMap<Column, Integer> column_index = new HashMap<>();
    for (int i = 0; i < len; i++) {
      column_index.put(this.columns.get(i), i);
    }
    for (Column column : this.columns) {
      int index = column_index.get(column);
      entry_list_sorted.add(entries.get(index));
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
  // TODO 后续考虑加入选择逻辑
  public void delete(Entry primary_entry, boolean in_tran) {
    if (primary_entry == null) return;
    try {
      lock.writeLock().lock();
      cache.deleteRow(primary_entry, primaryIndex, in_tran);
    } finally {
      lock.writeLock().unlock();
    }
  }

  // update函数更新主键值所对应的行
  // TODO 后续考虑加入选择逻辑
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

  // TODO
  public String delete(Logic the_logic) {
    int count = 0;
    for (Row row : this) {
      JointRow the_row = new JointRow(row, this);
      if (the_logic == null || the_logic.GetResult(the_row) == ResultType.TRUE) {
        Entry primary_entry = row.getEntries().get(primaryIndex);
        delete(primary_entry);
        count++;
      }
    }
    return "Deleted " + count + " items.";
  }

  public void delete(Entry primaryEntry) {
    if (primaryEntry == null) throw new KeyNotExistException(null);

    try {
      lock.writeLock().lock();
      cache.deleteRow(primaryEntry, primaryIndex, false);
    } catch (KeyNotExistException e) {
      throw e;
    } finally {
      lock.writeLock().unlock();
    }
  }

  // TODO 这个TableIterator类用于实现Table的迭代器，但是还不太理解有什么作用
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
