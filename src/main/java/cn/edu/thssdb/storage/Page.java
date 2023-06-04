package cn.edu.thssdb.storage;

import cn.edu.thssdb.schema.Entry;

import java.util.ArrayList;

public class Page {
  public static final int maxSize = 2048;
  private int id; // 每个页的id
  private int size; // 页面的实际大小
  private ArrayList<Entry> entry_list; // 这是行的主键，能够唯一表示这一行
  private String disk_filename; // 页面存储在磁盘当中的文件名
  private long time_stamp; // 更新时间戳
  private Boolean edited; // 编辑标记
  private Boolean in_tran; // 事务中标记

  public Page(String Name, int id) {
    this.id = id;
    size = 0;
    disk_filename =
        "page_" + Name + "_" + id + ".data"; // 存储数据的文件名 格式为 page_database_table_pageid.data
    entry_list = new ArrayList<>();
    time_stamp = System.currentTimeMillis();
    edited = false;
    in_tran = false;
  }

  public int getId() {
    return id;
  }

  public int getSize() {
    return size;
  }

  public ArrayList<Entry> getEntry_list() {
    return entry_list;
  }

  public void Entry_list_add(Entry entry, int len) {
    size += len;
    entry_list.add(entry);
  }

  public void Entry_list_remove(Entry entry, int len) {
    size -= len;
    entry_list.remove(entry);
  }

  public String getDisk_filename() {
    return disk_filename;
  }

  public void setTime_stamp() {
    this.time_stamp = System.currentTimeMillis();
  }

  public long getTime_stamp() {
    return time_stamp;
  }

  public void setEdited(Boolean edited) {
    this.edited = edited;
  }

  public Boolean getEdited() {
    return edited;
  }

  public void setIn_tran(Boolean in_tran) {
    this.in_tran = in_tran;
  }

  public Boolean getIn_tran() {
    return in_tran;
  }
}
