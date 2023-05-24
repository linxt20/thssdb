package cn.edu.thssdb.schema;

import cn.edu.thssdb.runtime.ServiceRuntime;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static cn.edu.thssdb.utils.Global.storage_dir;

public class Manager {
  private Database currentDB; // 当前正在使用的database
  private HashMap<String, Database> databases;
  private static ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
  // 补充事务和锁的数据结构

  public static Manager getInstance() {
    return Manager.ManagerHolder.INSTANCE;
  }

  public Manager() {
    // TODO
    System.out.println("Manager init");
    this.currentDB = null;
    this.databases = new HashMap<>();
    recover();
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
      // TODO throw new DatabaseNotExistException(dbName);
      currentDB = databases.get(dbName);
      System.out.println("Current database is " + dbName);
    } finally {
      lock.writeLock().unlock();
    }
  }
  // containdatabase函数是判断是否存在指定名字的database
  public boolean containDatabase(String dbName) {
    // TODO
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
      if (!databases.containsKey(dbName)) databases.put(dbName, new Database(dbName));
      if (currentDB == null) {
        try {
          lock.readLock().lock();
          if (!databases.containsKey(dbName))
            throw new RuntimeException("Database " + dbName + " not exist");
          // TODO throw new DatabaseNotExistException(dbName);
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
      ServiceRuntime.executeStatement("use " + databaseName);

      try (BufferedReader bufferedReader = new BufferedReader(new FileReader(file))) {
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
        int last_cmd = 0;
        if (transaction_list.size() == commit_list.size()) {
          last_cmd = index - 1;
        } else {
          last_cmd = transaction_list.get(transaction_list.size() - 1) - 1;
        }
        for (int i = 0; i <= last_cmd; i++) {
          ServiceRuntime.executeStatement(lines.get(i));
        }
        System.out.println("read " + (last_cmd + 1) + " lines");

        // 清空log并重写实际执行部分
        if (transaction_list.size() != commit_list.size()) {
          // 删除旧的日志文件
          Path path = Paths.get(log_name);
          Files.deleteIfExists(path);

          // 写入实际执行部分内容
          Files.write(path, stringBuilder.toString().getBytes(), StandardOpenOption.CREATE);
        }
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
}
