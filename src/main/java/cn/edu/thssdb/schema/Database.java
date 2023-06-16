package cn.edu.thssdb.schema;

import cn.edu.thssdb.exception.KeyNotExistException;
import cn.edu.thssdb.query.*;
import cn.edu.thssdb.type.ColumnType;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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

  public Table get(String name) {
    try {
      lock.readLock().lock();
      if (!tables.containsKey(name)) {
        throw new KeyNotExistException();
      }
      return tables.get(name);
    } finally {
      lock.readLock().unlock();
    }
  }

  public String getName() {
    return name;
  }

  // 这里的presist_table只写入单个table的元数据，之所以前面的persist不调用这个，是因为考虑减少函数调用的开销
  public void persist_table(Table table) {
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
      throw new RuntimeException("Database persist error");
    }
  }
  // 数据库的persist将元数据写入文件 格式为meta_database_table.data
  public void persist() {
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
        throw new RuntimeException("Database persist error");
      }
    }
  }

  // 在database中创建table
  public void create(String name, Column[] columns) {
    if (columns == null) {
      System.out.println("Database create table, columns is null");
      return;
    } else {
      for (Column column : columns)
        System.out.println("Database create table, " + column.toString());
    }
    try {
      lock.writeLock().lock();
      if (tables.containsKey(name))
        throw new RuntimeException("Database create table error: table already exists");
      Table new_table = new Table(this.name, name, columns);
      tables.put(name, new_table);
      persist_table(new_table); // 这里修改为只写入新建的table的元数据，不用重复写入所有table的元数据
    } finally {
      lock.writeLock().unlock();
    }
  }

  // 在database中删除名称为name的table
  public void drop(String name) {
    try {
      lock.writeLock().lock();
      if (!tables.containsKey(name)) // 判断table是否存在
      throw new RuntimeException("database don't have the table");
      // 这里使用StringBuilder来构造字符串，减少内存开销
      StringBuilder stringBuilder =
          new StringBuilder(storage_dir)
              .append("meta_")
              .append(this.name)
              .append("_")
              .append(name)
              .append(".data");
      File metaFile = new File(stringBuilder.toString());
      if (metaFile.isFile()) metaFile.delete();
      Table table = tables.get(name);
      table.dropSelf();
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
      // 这里使用StringBuilder来构造字符串，减少内存开销
      StringBuilder filenamePrefixBuilder =
          new StringBuilder(storage_dir).append("meta_").append(this.name).append("_");
      String filenameSuffix = ".data";
      List<File> filesToDelete = new ArrayList<>(); // 用于存储需要删除的文件
      for (Table table : tables.values()) {
        File metaFile =
            new File(filenamePrefixBuilder.toString() + table.tableName + filenameSuffix);
        if (metaFile.isFile()) {
          filesToDelete.add(metaFile);
        }
        table.dropSelf();
      }
      filesToDelete.forEach(File::delete); // 采用foreach的并行删除，减少系统的文件操作开销也提高了效率
      tables.clear();
      tables = null;
      tables = new HashMap<>();
    } finally {
      lock.writeLock().unlock();
    }
  }

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

  public String ToString() {
    // 迭代值
    String Top = "Database Name: " + name;
    String result = Top + "\n" + "\n";
    if (tables.isEmpty()) {
      return "Empty database!";
    }
    for (Table the_table : tables.values()) {
      if (the_table == null) {
        continue;
      }
      result += the_table.show();
    }
    return result;
  }

  /** 描述：建立单一querytable 参数：table name 返回：querytable */
  public QueryTable BuildSingleQueryTable(String table_name, Logic selectLogic) {
    try {
      lock.readLock().lock();
      if (tables.containsKey(table_name)) {
        return new SingleQueryTable(tables.get(table_name), selectLogic);
      }
    } finally {
      lock.readLock().unlock();
    }
    throw new KeyNotExistException();
  }
  /** 描述：建立复合querytable 参数：table names，join逻辑 返回：querytable */
  public QueryTable BuildJointQueryTable(ArrayList<String> table_names, Logic logic, int joinType, Logic selectLogic) {
    // 0: no join，1: left join，2: right join 3: full join 4: 正常inner join
    ArrayList<Table> my_tables = new ArrayList<>();
    try {
      lock.readLock().lock();
      for (String table_name : table_names) {
        if (!tables.containsKey(table_name)) throw new KeyNotExistException();
        my_tables.add(tables.get(table_name));
      }
      return new MultiQueryTable(my_tables, logic, joinType, selectLogic);
    } catch (Exception e) {
      throw e;
    } finally {
      lock.readLock().unlock();
    }
  }
  // todo 这里还需要再看看
  public QueryResult select(
      String[] columnsProjected, QueryTable the_table, boolean distinct) {
    try {
      lock.readLock().lock();
      QueryResult query_result = new QueryResult(the_table, columnsProjected, distinct);
      query_result.GenerateQueryRecords();
      return query_result;
    } catch (KeyNotExistException e) {
      throw e;
    } finally {
      lock.readLock().unlock();
    }
  }

  public void insert(String tableName, String[] column_names, String[] values) {
    try {
      lock.writeLock().lock();
      Table table = get(tableName);
      if (column_names == null) {
        for (int i = 0; i < values.length; i++) {
          System.out.println("Database insert, " + values[i]);
        }
        table.insert(values, false);
      } else {
        for (int i = 0; i < values.length; i++) {
          System.out.println("Database insert, " + values[i]);
        }
        table.insert(column_names, values, false);
      }
    } catch (RuntimeException e) {
      e.printStackTrace();
      throw e;
    } finally {
      lock.writeLock().unlock();
    }
  }

  public String update(String table_name, String column_name, Comparer value, Logic the_logic) {
    Table the_table = get(table_name);
    try {
      return the_table.update(column_name, value, the_logic);
    } catch (RuntimeException e) {
      e.printStackTrace();
      throw e;
    }
  }

  public String delete(String table_name, Logic the_logic) {
    Table table = get(table_name);
    return table.delete(the_logic);
  }

  private void recover() {
    File dir = new File(storage_dir);
    if (!dir.exists() || !dir.isDirectory()) return;
    File[] fileList = dir.listFiles();
    if (fileList == null) return;

    String metaPrefix = "meta_";
    Pattern fileNamePattern = Pattern.compile("^meta_" + this.name + "_(.*?)\\.data$");

    for (File f : fileList) {
      if (!f.isFile()) {
        continue;
      }
      // 这是一种try-with-resources的写法，可以自动关闭资源
      try (FileReader fileReader = new FileReader(f);
          BufferedReader bufferedReader = new BufferedReader(fileReader)) {
        // 这里使用正则表达式来匹配文件名
        Matcher matcher = fileNamePattern.matcher(f.getName());
        if (!matcher.matches()) {
          continue;
        }
        // 这里用matcher的group方法来获取匹配到的table名字
        String tableName = matcher.group(1);
        if (tables.containsKey(tableName)) {
          throw new RuntimeException("Duplicate table name: " + tableName);
        }

        ArrayList<Column> columns = new ArrayList<>();
        String line;
        while ((line = bufferedReader.readLine()) != null) {
          String[] info = line.split(",");
          String columnName = info[0];
          ColumnType columnType = ColumnType.valueOf(info[1]);
          int primaryKey = Integer.parseInt(info[2]);
          boolean notNull = Boolean.parseBoolean(info[3]);
          int maxLen = Integer.parseInt(info[4]);
          Column column = new Column(columnName, columnType, primaryKey, notNull, maxLen);
          columns.add(column);
        }

        Table table = new Table(this.name, tableName, columns.toArray(new Column[0]));
        tables.put(tableName, table);

      } catch (Exception e) {
        continue;
      }
    }
  }
  // 这个quit是退出客户端的时候调用的，这里会将所有的表都持久化
  public void quit() {
    try {
      lock.writeLock().lock();
      for (Table table : tables.values()) {
        table.persist();
      }
      persist();
    } catch (Exception e) {
      throw e;
    } finally {
      lock.writeLock().unlock();
    }
  }

  public List<String> getTableNames() {
    try {
      lock.readLock().lock();
      return new ArrayList<>(tables.keySet());
    } finally {
      lock.readLock().unlock();
    }
  }
}
