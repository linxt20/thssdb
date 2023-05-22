package cn.edu.thssdb.schema;

import cn.edu.thssdb.index.BPlusTree;
import cn.edu.thssdb.storage.Cache;
import cn.edu.thssdb.utils.Pair;

import java.io.File;
import java.io.FileInputStream;
import java.io.ObjectInputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static cn.edu.thssdb.utils.Global.storage_dir;

public class Table implements Iterable<Row> {
  ReentrantReadWriteLock lock;
  private String databaseName;
  public String tableName;
  public ArrayList<Column> columns;
  private int primaryIndex;
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

  private void recover() {
    // TODO 需要修改
    File dir = new File(storage_dir);
    File[] fileList = dir.listFiles();
    if (fileList == null)
      return;

    HashMap<Integer, File> pageFileList = new HashMap<>();
    int pageNum = 0;
    for (File f : fileList) {
      if (f != null && f.isFile()) {
        try {

          String[] parts = f.getName().split("\\.")[0].split("_");

          String databaseName = parts[1];
          String tableName = parts[2];

          int id = Integer.parseInt(parts[3]);
          if (!(this.databaseName.equals(databaseName) && this.tableName.equals(tableName)))
            continue;
          pageFileList.put(id, f);
          if (id > pageNum)
            pageNum = id;
        } catch (Exception e) {
          continue;
        }
      }
    }

    for (int i = 1; i <= pageNum; i++) {

      File f = pageFileList.get(i);
      ArrayList<Row> rows = deserialize(f);
      // TODO cache.insertPage(rows, primaryIndex);
    }
  }

  public String show() {
    String ret = "------------------------------------------------------\n";
    for (Column column : columns) {
      ret += column.show() + "\n";
    }
    ret += "------------------------------------------------------";
    return ret;
  }

  public void insert() {
    // TODO
  }

  public void delete() {
    // TODO
  }

  public void update() {
    // TODO
  }

  private void serialize() {
    // TODO
  }

  public void dropSelf() {
    try {
      lock.writeLock().lock();
      // TODO cache
      // cache.dropSelf();
      // cache = null;

      File dir = new File(storage_dir);
      File[] fileList = dir.listFiles();
      if (fileList == null)
        return;
      for (File f : fileList) {
        if (f != null && f.isFile()) {
          try {
            String[] parts = f.getName().split("\\.")[0].split("_");
            String databaseName = parts[1];
            String tableName = parts[2];
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

  private ArrayList<Row> deserialize(File file) {
    ArrayList<Row> rows;
    try {
      ObjectInputStream ois = new ObjectInputStream(new FileInputStream(file));
      rows = (ArrayList<Row>) ois.readObject();
      ois.close();
    } catch (Exception e) {
      rows = null;
    }
    return rows;
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
