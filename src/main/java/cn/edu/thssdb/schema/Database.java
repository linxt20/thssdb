package cn.edu.thssdb.schema;

import cn.edu.thssdb.exception.DuplicateKeyException;
import cn.edu.thssdb.exception.KeyNotExistException;
import cn.edu.thssdb.query.QueryResult;
import cn.edu.thssdb.query.QueryTable;

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

  public void drop() {
    System.out.println("Database drop");
    // TODO 需要修改
    try {
      lock.writeLock().lock();
      if (!tables.containsKey(name)) throw new KeyNotExistException();
      // TODO throw new TableNotExistException(name);
      String metaFilename = storage_dir + "meta_" + this.name + "_" + name + ".data";
      File metaFile = new File(metaFilename);
      if (metaFile.isFile()) metaFile.delete();
      Table table = tables.get(name);
      // table.dropall();
      tables.remove(name);
    } finally {
      lock.writeLock().unlock();
    }
  }

  // 展示database中tableName表中的元数据
  public String show(String tableName) {
    try {
      lock.readLock().lock();
      if (!tables.containsKey(tableName)) throw new KeyNotExistException();
      Table table = tables.get(tableName);
      return table.show();
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
        //        table.insert(values);
      } else {
        //        table.insert(column_names, values);
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

  public String select(QueryTable[] queryTables) {
    // TODO
    QueryResult queryResult = new QueryResult(queryTables);
    return null;
  }

  private void recover() {
    // TODO
  }

  public void quit() {
    // TODO
  }
}
