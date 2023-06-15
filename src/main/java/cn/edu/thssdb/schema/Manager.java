package cn.edu.thssdb.schema;

import cn.edu.thssdb.runtime.ServiceRuntime;

import java.io.*;
import java.nio.file.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static cn.edu.thssdb.utils.Global.storage_dir;

public class Manager {
  private Database currentDB; // 当前正在使用的database
  private HashMap<String, Database> databases;
  private static ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

  public ArrayList<Long> transaction_sessions; // 处于transaction状态的session列表
  public ArrayList<Long> session_queue; // 由于锁阻塞的session队列
  public HashMap<Long, ArrayList<String>> s_lock_dict; // 记录每个session取得了哪些表的s锁
  public HashMap<Long, ArrayList<String>> x_lock_dict; // 记录每个session取得了哪些表的x锁

  public static Manager getInstance() {
    return Manager.ManagerHolder.INSTANCE;
  }

  public Manager() {
    System.out.println("Manager init");
    this.currentDB = null;
    this.databases = new HashMap<>();
    transaction_sessions = new ArrayList<>();
    session_queue = new ArrayList<>();
    s_lock_dict = new HashMap<>();
    x_lock_dict = new HashMap<>();
  }

  // getdatabase函数是获取指定名字的database
  public Database getDatabase(String dbName) {
    try {
      lock.readLock().lock();
      if (!databases.containsKey(dbName))
        throw new RuntimeException("Database " + dbName + " not exist");
      return databases.get(dbName);
    } finally {
      lock.readLock().unlock();
    }
  }

  // getcurrentdb函数是获取当前正在使用的database
  public Database getCurrentDB() {
    try {
      lock.readLock().lock();
      return currentDB;
    } finally {
      lock.readLock().unlock();
    }
  }

  // setcurrentdb函数是设置当前正在使用的database
  public void setCurrentDB(String dbName) {
    // 设置当前正在使用的database
    try {
      lock.writeLock().lock();
      if (!databases.containsKey(dbName))
        throw new RuntimeException("Database " + dbName + " not exist");
      currentDB = databases.get(dbName);
      System.out.println("Current database is " + dbName);
    } finally {
      lock.writeLock().unlock();
    }
  }

  // containdatabase函数是判断是否存在指定名字的database
  public boolean containDatabase(String dbName) {
    try {
      lock.readLock().lock();
      return databases.containsKey(dbName);
    } finally {
      lock.readLock().unlock();
    }
  }

  // createdatabaseifnotexists函数是创建指定名字的database
  public void createDatabaseIfNotExists(String dbName) {
    try {
      lock.writeLock().lock();
      if (!databases.containsKey(dbName))
        databases.put(dbName, new Database(dbName));
      if (currentDB == null) {
        try {
          lock.readLock().lock();
          if (!databases.containsKey(dbName))
            throw new RuntimeException("Database " + dbName + " not exist");
          currentDB = databases.get(dbName);
        } finally {
          lock.readLock().unlock();
        }
      }
    } finally {
      lock.writeLock().unlock();
    }
  }

  // deletedatabase函数是删除指定名字的database
  public void deleteDatabase(String dbName) {
    try {
      lock.writeLock().lock();
      if (!databases.containsKey(dbName))
        throw new RuntimeException("Database " + dbName + " not exist");
      // 调用 database.dropall() 删除数据库内所有的元数据文件和存储数据的页文件
      databases.get(dbName).dropall();
      databases.remove(dbName);
    } finally {
      lock.writeLock().unlock();
    }
  }

  public void write_log(String log_line) {
    Database current_base = getCurrentDB();
    String database_name = current_base.getName();
    String filename = storage_dir + database_name + ".log";
    try (BufferedWriter writer = new BufferedWriter(new FileWriter(filename, true))) {
      System.out.println("write log: " + log_line);
      writer.write(log_line);
      writer.newLine();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public void read_log(String databaseName) {
    String log_name = storage_dir + getCurrentDB().getName() + ".log";
    File file = new File(log_name);
    if (file.exists() && file.isFile()) {
      System.out.println("log file size: " + file.length() + " Byte");
      System.out.println("Read WAL log to recover database.");
      String temp_log_name = storage_dir + getCurrentDB().getName() + ".temp.log";
      File temp_file = new File(temp_log_name);
      try {
        Files.copy(file.toPath(), temp_file.toPath(), StandardCopyOption.REPLACE_EXISTING);
      } catch (IOException e) {
        e.printStackTrace();
      }

      // 清空原日志文件
      try (PrintWriter writer = new PrintWriter(file)) {
        writer.print("");
      } catch (FileNotFoundException e) {
        e.printStackTrace();
      }

      ServiceRuntime.executeStatement("use " + databaseName, 0);

      try (BufferedReader bufferedReader = new BufferedReader(new FileReader(temp_file))) {
        String line;
        ArrayList<String> lines = new ArrayList<>();
        StringBuilder stringBuilder = new StringBuilder();
        ArrayList<Integer> transaction_list = new ArrayList<>();
        ArrayList<Integer> commit_list = new ArrayList<>();
        int index = 0;
        while ((line = bufferedReader.readLine()) != null) {
          if (line.equals("begin transaction")) {
            transaction_list.add(index);
          } else if (line.equals("commit")) {
            commit_list.add(index);
          }
          lines.add(line);
          stringBuilder.append(line).append("\n");
          index++;
        }
        int start_cmd = 0;

        if (transaction_list.size() != commit_list.size()) {
          start_cmd = transaction_list.get(transaction_list.size() - 1);
          // // redo
          // for (int i = start_cmd; i < index; i++) {
          // ServiceRuntime.executeStatement(lines.get(i),0);
          // }
          // // undo
          // for (int i = index-1; i >= start_cmd; i--) {
          // ServiceRuntime.executeStatement(lines.get(i),0);
          // }
        }

        System.out.println("read " + (index - start_cmd + 1) + " lines");
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
  }

  public void recover() {
    File managerFile = new File(storage_dir + "manager.data");
    if (!managerFile.isFile()) {
      return;
    }
    try (BufferedReader bufferedReader = new BufferedReader(new FileReader(managerFile))) {
      String line;
      while ((line = bufferedReader.readLine()) != null) {
        createDatabaseIfNotExists(line);
        read_log(line);
      }
    } catch (IOException e) {
      throw new RuntimeException(storage_dir + "manager.data");
    }
  }

  public void persist() {
    try (BufferedWriter writer = new BufferedWriter(new FileWriter(storage_dir + "manager.data"))) {
      for (String databaseName : databases.keySet()) {
        writer.write(databaseName);
        writer.newLine();
      }
    } catch (IOException e) {
      throw new RuntimeException(storage_dir + "manager.data");
    }
  }

  public void persist_database(String dbName) {
    try {
      lock.writeLock().lock();
      Database db = databases.get(dbName);
      db.quit();
      persist();
    } finally {
      lock.writeLock().unlock();
    }
  }

  public void quit() {
    try {
      lock.writeLock().lock();
      for (Database db : databases.values()) {
        db.quit();
      }
      persist();
      databases.clear();
    } finally {
      lock.writeLock().unlock();
    }
  }

  private static class ManagerHolder {
    private static final Manager INSTANCE = new Manager();

    private ManagerHolder() {}
  }

  public String showDatabases() {
    StringBuilder stringBuilder = new StringBuilder();
    for (String databaseName : databases.keySet()) {
      stringBuilder.append(databaseName).append("\n");
    }
    return stringBuilder.toString();
  }

  public String showDatabaseMeta(String databaseName) {
    Database database = databases.get(databaseName);
    if (database == null) {
      throw new RuntimeException("Database " + databaseName + " not exist");
    }
    // 对database的每一个table，将其meta信息打印出来
    StringBuilder stringBuilder = new StringBuilder();
    for (String tableName : database.getTableNames()) {
      stringBuilder.append(tableName).append("\n").append(database.show(tableName)).append("\n");
    }
    return stringBuilder.toString();
  }
}
