package cn.edu.thssdb.schema;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.StringJoiner;

public class Row implements Serializable {
  private static final long serialVersionUID = -5809782578272943999L;
  protected ArrayList<Entry> entries;

  protected int position; // 记录在哪一个页面当中

  public Row() {
    this.entries = new ArrayList<>();
    position = 0;
  }

  public Row(Entry[] entries) {
    this.entries = new ArrayList<>(Arrays.asList(entries));
    position = 0;
  }

  // 这里是为了外连接专门构造一个所以entries都为null的Row
  public Row(int count){
    this.entries = new ArrayList<>();
    for (int i = 0; i < count; i++) {
      entries.add(new Entry(null));
    }
  }

  public ArrayList<Entry> getEntries() {
    return entries;
  }

  public int getPosition() {
    return position;
  }

  public void setPosition(int position) {
    this.position = position;
  }

  public String toString() {
    if (entries == null) return "EMPTY";
    StringJoiner sj = new StringJoiner(", ");
    for (Entry e : entries)
      sj.add(e.toString()); // 这里的e.toString()是Entry类的toString()方法，在Entry当中实现了null转化为字符串
    return sj.toString();
  }

  public ArrayList<String> toStringList() {
    ArrayList<String> result = new ArrayList<>();
    if (entries == null) return result;
    for (Entry e : entries) result.add(e.toString());
    return result;
  }
}
