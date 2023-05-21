package cn.edu.thssdb.storage;

import cn.edu.thssdb.exception.DuplicateKeyException;
import cn.edu.thssdb.exception.KeyNotExistException;
import cn.edu.thssdb.index.BPlusTree;
import cn.edu.thssdb.schema.Entry;
import cn.edu.thssdb.schema.Row;
import cn.edu.thssdb.utils.Pair;

import java.io.*;
import java.util.*;

import static cn.edu.thssdb.utils.Global.storage_dir;

public class Cache {
  private static final int max_num = 1024; // 页的最大数量，超过将会使用页面置换算法
  private int num; // 计数器，作为页面的id
  private LinkedHashMap<Integer, Page> page_map; // 这里使用linkedhashmap的数据结构，是一个双向链表，简化LRU的页面置换算法

  private BPlusTree<Entry, Row> index; // 索引
  private String cache_name;

  public Cache(String database_name, String table_name) {
    num = 0;
    page_map = new LinkedHashMap<>();
    index = new BPlusTree<>();
    cache_name = database_name + "_" + table_name;
  }

  public Iterator<Pair<Entry, Row>> getIndexIter() {
    return index.iterator();
  }
  // 这是一个空行的子类，标记了这个空行在哪个page上
  public class emptyRow extends Row {
    public emptyRow(int pos) {
      super();
      this.position = pos;
    }
  }
  // 这是数据序列化，写入到磁盘当中
  private void data_serialize(ArrayList<Row> row_list, String filename) {
    try {
      FileOutputStream output_file = new FileOutputStream(filename);
      ObjectOutputStream outputStream = new ObjectOutputStream(output_file);
      outputStream.writeObject(row_list);
      outputStream.close();
    } catch (IOException e) {
      throw new RuntimeException("Error while serializing data", e);
    }
  }
  // 反序列化，就是从磁盘读数据转化为行数据
  private ArrayList<Row> data_deserialize(File file) {
    ArrayList<Row> row_list;
    ObjectInputStream inputStream = null;
    try {
      inputStream = new ObjectInputStream(new FileInputStream(file));
      row_list = (ArrayList<Row>) inputStream.readObject();
      inputStream.close();
      return row_list;
    } catch (Exception e) {
      try {
        inputStream.close();
        return null;
      } catch (IOException ex) {
        return null;
      }
    }
  }
  // 删除最久没用到的页面LRU，如果页面有被编辑过，需要写入磁盘
  public void removePage() {
    Iterator<Map.Entry<Integer, Page>> iter = page_map.entrySet().iterator(); // 这里是使用LRU的页面置换算法
    if (iter.hasNext()) {
      Page oldest = iter.next().getValue();
      if (oldest == null) {
        return;
      }
      ArrayList<Row> row_list = new ArrayList<>();
      ArrayList<Entry> entry_list = oldest.getEntry_list();
      // 拷贝要被删除的页面的信息
      for (Entry entry : entry_list) {
        row_list.add(index.get(entry));
        index.update(entry, this.new emptyRow(oldest.getId()));
      }
      if (oldest.getEdited()) {
        data_serialize(row_list, storage_dir + oldest.getDisk_filename());
      }
      iter.remove();
    }
  }
  // 页面置换，先删除最久的页面，然后将新的行从底盘读出来，写到磁盘当中
  private void exchangePage_LRU(int id, int primary_key) {
    if (num >= max_num) {
      removePage();
    }
    Page temp_page = new Page(cache_name, id);
    File temp_file = new File(storage_dir + temp_page.getDisk_filename());
    ArrayList<Row> row_list = data_deserialize(temp_file);
    for (Row row : row_list) {
      row.setPosition(id);
      Entry primary_entry = row.getEntries().get(primary_key);
      index.update(primary_entry, row);
      int length = row.toString().length();
      temp_page.Entry_list_add(primary_entry, length);
    }
    page_map.put(id, temp_page);
  }
  // 添加页面，检查页面数量
  public boolean addPage() {
    boolean overflow = false;
    if (num >= max_num) {
      removePage();
      overflow = true;
    }
    num++;
    Page new_page = new Page(cache_name, num);
    page_map.put(num, new_page);
    return overflow;
  }
  // 将从磁盘当中读取的行信息插入到新的页当中
  public void insertPage(ArrayList<Row> row_list, int primary_key) {
    addPage();
    Page temp_page = page_map.get(num);
    for (Row row : row_list) {
      ArrayList<Entry> entry_list = row.getEntries(); // 这个地方get到的是所有的键
      Entry primary_entry = entry_list.get(primary_key);
      int length = row.toString().length();
      temp_page.Entry_list_add(primary_entry, length);
      row.setPosition(num);
      index.put(primary_entry, row);
    }
  }
  // 这是将行信息插入到页面当中，先获取键值信息，然后获取页面信息，最后写入，需要修改页面的时间戳、编辑状态
  public void insertRow(ArrayList<Entry> entry_list, int primary_key, boolean in_tran) {
    Entry[] entryArray = new Entry[entry_list.size()];
    for (int i = 0; i < entry_list.size(); i++) {
      entryArray[i] = entry_list.get(i);
    }
    Row row = new Row(entryArray);
    int length = row.toString().length();
    Entry primary_entry = entry_list.get(primary_key);
    Page temp_page = page_map.get(num);
    if (temp_page == null || ((temp_page.getSize() + length) > Page.maxSize)) {
      addPage();
      temp_page = page_map.get(num);
    }
    row.setPosition(num);
    try {
      index.put(primary_entry, row);
    } catch (DuplicateKeyException e) { // 如果主键冲突
      temp_page.setTime_stamp();
      throw new DuplicateKeyException(primary_entry.toString());
    }
    temp_page.Entry_list_add(primary_entry, length);
    temp_page.setEdited(true);
    temp_page.setTime_stamp();
    if (in_tran) {
      temp_page.setIn_tran(true);
    }
  }
  // 获取行，需要先检查是否是空行，空行得去内存里面找
  public Row getRow(Entry primary_entry, int primary_key) {
    Row row;
    try {
      row = index.get(primary_entry);
    } catch (KeyNotExistException e) { // 如果主键不存在
      throw new KeyNotExistException(primary_entry.toString());
    }

    int position = row.getPosition();

    // 判断row是不是emptyRow的类或者子类对象,如果为空，表明此时这个行所在的页面在磁盘当中
    if (row instanceof emptyRow) {
      exchangePage_LRU(position, primary_key);
      row = index.get(primary_entry);
    } else {
      page_map.get(position).setTime_stamp();
    }
    return row;
  }
  // 删除行，先获取行，然后在索引和页面当中删除相应数据即可
  public void deleteRow(Entry entry, int primary_key, boolean in_tran) {
    Row row = getRow(entry, primary_key);
    index.remove(entry);
    Page temp_page = page_map.get(row.getPosition());
    int length = row.toString().length();
    temp_page.Entry_list_remove(entry, length);
    temp_page.setEdited(true);
    if (in_tran) {
      temp_page.setIn_tran(true);
    }
  }
  // 更新行，先判断行是否在内存，然后判断主键是否被修改，修改需要判断主键冲突，然后更新行数据,最后更新页数据，需要注意页数据需要行数据才能更新，顺序不能错误
  public void updateRow(
      Entry primary_entry,
      int primary_key,
      int[] target_key_list,
      ArrayList<Entry> target_entry_list,
      boolean in_tran) {
    Row row = getRow(primary_entry, primary_key);
    boolean isPrimaryKeyChanged = false;
    Entry target_primary_entry = null;
    int length = row.toString().length();
    // 判断需要更新的键值位置是否包含主键的键值位置
    for (int i = 0; i < target_key_list.length; i++) {
      if (target_key_list[i] == primary_key) {
        isPrimaryKeyChanged = true;
        target_primary_entry = target_entry_list.get(i);
        if (index.contains(target_primary_entry) && !primary_entry.equals(target_primary_entry)) {
          throw new DuplicateKeyException(target_primary_entry.toString());
        }
        break;
      }
    }
    ArrayList<Entry> temp_entry_list = row.getEntries();
    // 更新行内容
    for (int i = 0; i < target_key_list.length; i++) {
      temp_entry_list.set(target_key_list[i], target_entry_list.get(i));
    }
    // 更新页的标记
    Page temp_page = page_map.get(row.getPosition());
    if (isPrimaryKeyChanged) {
      temp_page.Entry_list_remove(primary_entry, length);
      temp_page.Entry_list_add(target_primary_entry, row.toString().length());
      index.remove(primary_entry);
      index.put(target_primary_entry, row);
    }
    temp_page.setEdited(true);
    if (in_tran) {
      temp_page.setIn_tran(true);
    }
  }
  // 数据持久化
  public void data_persist() {
    for (Page page : page_map.values()) {
      ArrayList<Row> row_list = new ArrayList<>();
      for (Entry entry : page.getEntry_list()) {
        row_list.add(index.get(entry));
      }
      data_serialize(row_list, storage_dir + page.getDisk_filename());
    }
  }
  // 删除表所有的数据,这种简洁的写法有个问题，需要在其他地方保证没有对于index和page的引用，也就是不能存在对于其中内容的引用变量作为了全局变量
  public void dropall() {
    for (Page page : page_map.values()) {
      page.getEntry_list().clear();
    }
    page_map.clear();
    index = null;
  }
  // 去除事务状态
  public void quit_tran() {
    for (Page page : page_map.values()) {
      page.setIn_tran(false);
    }
  }
}
