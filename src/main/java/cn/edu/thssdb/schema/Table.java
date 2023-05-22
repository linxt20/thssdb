package cn.edu.thssdb.schema;

import cn.edu.thssdb.index.BPlusTree;
import cn.edu.thssdb.utils.Pair;

import java.io.File;
import java.io.FileInputStream;
import java.io.ObjectInputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static cn.edu.thssdb.utils.Global.STORE_DIRECTORY;

public class Table implements Iterable<Row> {
  ReentrantReadWriteLock lock;
  private String databaseName;
  public String tableName;
  public ArrayList<Column> columns;
  public BPlusTree<Entry, Row> index;
  private int primaryIndex;

  public Table(String databaseName, String tableName, Column[] columns) {
    System.out.println("==========Table constructor=============");
    // TODO 需要修改
    this.databaseName = databaseName;
    this.tableName = tableName;
    this.columns = new ArrayList<>(Arrays.asList(columns));
    for (int i = 0; i < this.columns.size(); i++) {
      if (this.columns.get(i).getPrimary() == 1) primaryIndex = i;
    }
    if (primaryIndex < 0 || primaryIndex >= this.columns.size()) {
      System.out.println("Primary key not exist");
      // TODO throw new PrimaryNotExistException(tableName);
    }
    this.lock = new ReentrantReadWriteLock();

    // TODO 一些后面要加的变量
    //    this.cache = new Cache(databaseName, tableName);
    //    this.s_lock_list = new ArrayList<>();
    //    this.x_lock_list = new ArrayList<>();
    //    this.tplock = 0;
    recover();
  }

  public String show() {
    String ret = "------------------------------------------------------\n";
    for (Column column : columns) {
      ret += column.show() + "\n";
    }
    ret += "------------------------------------------------------\n";
    return ret;
  }

  private void recover() {
    // TODO 需要修改
    File dir = new File(STORE_DIRECTORY);
    File[] fileList = dir.listFiles();
    if (fileList == null) return;

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
          if (id > pageNum) pageNum = id;
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

      File dir = new File(STORE_DIRECTORY);
      File[] fileList = dir.listFiles();
      if (fileList == null) return;
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
      this.iterator = table.index.iterator();
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
