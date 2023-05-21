package cn.edu.thssdb.schema;

import cn.edu.thssdb.exception.KeyNotExistException;

import java.util.HashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class Manager {
  private Database currentDB; // 当前正在使用的database
  private HashMap<String, Database> databases;
  private static ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

  public static Manager getInstance() {
    return Manager.ManagerHolder.INSTANCE;
  }

  public Manager() {
    // TODO
    this.currentDB = null;
    this.databases = new HashMap<>();
  }

  public Database getCurrentDB() {
    // 返回当前正在使用的database
    try {
      lock.readLock().lock();
      return currentDB;
    }
    finally {
      lock.readLock().unlock();
    }
  }

  public void setCurrentDB(String dbName) {
    // 设置当前正在使用的database
    try {
      lock.writeLock().lock();
      if (!databases.containsKey(dbName))
        throw new KeyNotExistException();
        // TODO throw new DatabaseNotExistException(dbName);
      currentDB = databases.get(dbName);
      System.out.println("Current database is " + dbName);
    }
    finally {
      lock.writeLock().unlock();
    }
  }


  public boolean containDatabase(String dbName) {
    // TODO
    try {
      lock.readLock().lock();
      return databases.containsKey(dbName);
    }
    finally {
      lock.readLock().unlock();
    }
  }
  public void createDatabaseIfNotExists(String dbName) {
    // TODO
    try {
      lock.writeLock().lock();
      if (!databases.containsKey(dbName))
        databases.put(dbName, new Database(dbName));
      if (currentDB == null) {
        try {
          lock.readLock().lock();
          if (!databases.containsKey(dbName))
            throw new KeyNotExistException();
            // TODO throw new DatabaseNotExistException(dbName);
          currentDB = databases.get(dbName);
        }
        finally {
          lock.readLock().unlock();
        }
      }
    }
    finally {
      lock.writeLock().unlock();
    }
  }

  public void deleteDatabase(String dbName) {
    // TODO
    try {
      lock.writeLock().lock();
      if (!databases.containsKey(dbName))
        throw new KeyNotExistException();
        // TODO throw new DatabaseNotExistException(dbName);
      databases.remove(dbName);
      // TODO 删除对应的文件夹（调用 database.dropSelf()
    }
    finally {
      lock.writeLock().unlock();
    }
  }

  public void switchDatabase() {
    // TODO
  }

  private static class ManagerHolder {
    private static final Manager INSTANCE = new Manager();

    private ManagerHolder() {}
  }
}
