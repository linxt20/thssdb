package cn.edu.thssdb.schema;

import cn.edu.thssdb.exception.DuplicateKeyException;
import cn.edu.thssdb.exception.KeyNotExistException;
import cn.edu.thssdb.query.*;

import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static cn.edu.thssdb.utils.Global.storage_dir;

public class Database {

  private String name; // 数据库名
  private HashMap<String, Table> tables; // 表名到表的映射
  ReentrantReadWriteLock lock; // 读写锁

  public Database(String name) {
    this.name = name;
    this.tables = new HashMap<>();
    this.lock = new ReentrantReadWriteLock();
    recover();
  }

  private void persist() {
    System.out.println("Database persist");
    // TODO 需要修改
    for (Table table : tables.values()) {
      String filename = storage_dir + "meta_" + name + "_" + table.tableName + ".data";
      ArrayList<Column> columns = table.columns;
      try {
        FileOutputStream fos = new FileOutputStream(filename);
        OutputStreamWriter writer = new OutputStreamWriter(fos);
        for (Column column : columns) {
          writer.write(column.toString() + "\n");
        }
        writer.close();
        fos.close();
      } catch (Exception e) {
        // TODO: throw new FileIOException(filename);
      }
    }
  }

  // 在database中创建table
  public void create(String name, Column[] columns) {
    if (columns == null) {
      System.out.println("Database create table, columns is null");
      return;
    } else
      for (Column column : columns)
        System.out.println("Database create table, " + column.toString());
    // TODO 需要修改
    try {
      lock.writeLock().lock();
      if (tables.containsKey(name))
        // TODO: throw exception DuplicateTableException, which is not defined yet
        throw new DuplicateKeyException();

      Table newTable = new Table(this.name, name, columns);
      tables.put(name, newTable);
      persist();
    } finally {
      lock.writeLock().unlock();
    }
  }

  // 在database中删除名称为name的table
  public void drop(String name) {
    System.out.println("Database drop table " + name);
    // TODO 需要修改
    try {
      lock.writeLock().lock();
      if (!tables.containsKey(name))
        throw new KeyNotExistException();
      // TODO throw new TableNotExistException(name);
      String metaFilename = storage_dir + this.name + "_" + name + "_meta.data";
      File metaFile = new File(metaFilename);
      if (metaFile.isFile())
        metaFile.delete();
      Table table = tables.get(name);
      table.dropall();
      // 将table从database中删除
      tables.remove(name);
    } finally {
      lock.writeLock().unlock();
    }
  }

  // 清空自己
  public void dropall() {
    try {
      lock.writeLock().lock();
      // TODO 需要修改一下这里的字符串处理方式
      final String filenamePrefix = storage_dir + this.name + "_";
      final String filenameSuffix = "_meta.data";
      for (Table table : tables.values()) {
        File metaFile = new File(filenamePrefix + table.tableName + filenameSuffix);
        if (metaFile.isFile())
          metaFile.delete();
        table.dropSelf();
      }
      tables.clear();
      tables = null;
    } finally {
      lock.writeLock().unlock();
    }
  }

  // 展示database中tableName表中的元数据
  public String show(String tableName) {
    try {
      lock.readLock().lock();
      if (!tables.containsKey(tableName))
        throw new KeyNotExistException();
      Table table = tables.get(tableName);
      return table.show();
    } finally {
      lock.readLock().unlock();
    }
  }

  /**
   * 描述：建立单一querytable
   * 参数：table name
   * 返回：querytable
   */
  public QueryTable BuildSingleQueryTable(String table_name) {
    try {
      lock.readLock().lock();
      if (tables.containsKey(table_name)) {
        return new SingleTable(tables.get(table_name));
      }
    } finally {
      lock.readLock().unlock();
    }
    throw new KeyNotExistException();
  }

  /**
   * 描述：建立复合querytable
   * 参数：table names，join逻辑
   * 返回：querytable
   */
  public QueryTable BuildJointQueryTable(ArrayList<String> table_names, Logic logic) {
    ArrayList<Table> my_tables = new ArrayList<>();
    try {
      lock.readLock().lock();
      for (String table_name : table_names) {
        if (!tables.containsKey(table_name))
          throw new KeyNotExistException();
        my_tables.add(tables.get(table_name));
      }
    } finally {
      lock.readLock().unlock();
    }
    return new JointTable(my_tables, logic);
  }

  /**
   * 描述：处理更新元素
   * 参数：table name，待更新的单一列名，待更新的值，符合条件
   * 返回：描述性语句
   */
  public String update(String table_name, String column_name, Comparer value, Logic the_logic) {
    Table the_table = get(table_name);
    return the_table.update(column_name, value, the_logic);
  }

  public QueryResult select(String[] columnsProjected, QueryTable the_table, Logic select_logic, boolean distinct) {
    try {

      lock.readLock().lock();
      the_table.SetLogicSelect(select_logic);
      QueryResult query_result = new QueryResult(the_table, columnsProjected, distinct);
      query_result.GenerateQueryRecords();
      return query_result;
    } finally {
      lock.readLock().unlock();
    }
  }
  public void insert(String tableName, String[] column_names, String[] values) {
    System.out.println("Database insert");
    // TODO 需要修改
    try {
      Table table = get(tableName);
      if (column_names == null) {
        for(int i = 0; i < values.length; i++) {
          System.out.println("Database insert, " + values[i]);
        }
        table.insert(values);
      } else {
        for(int i = 0; i < values.length; i++) {
          System.out.println("Database insert, " + values[i]);
        }
        table.insert(column_names, values);
      }
    } finally {
      lock.writeLock().unlock();
    }
  }

  public Table get(String name) {
    try {
      lock.readLock().lock(); // TODO 为什么是read？
      if (!tables.containsKey(name)) {
        throw new KeyNotExistException();
      }
      return tables.get(name);
    } finally {
      lock.readLock().unlock();
    }
  }

  private void recover() {
    // TODO
  }

  public void quit() {
    // TODO
  }
}
