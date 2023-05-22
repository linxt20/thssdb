package cn.edu.thssdb.schema;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.StringJoiner;

public class Row implements Serializable {
  private static final long serialVersionUID = -5809782578272943999L;
  protected ArrayList<Entry> entries;

  protected int position; // 记录在哪一个

  public Row() {
    this.entries = new ArrayList<>();
    position = 0;
  }

  public Row(Entry[] entries) {
    this.entries = new ArrayList<>(Arrays.asList(entries));
    position = 0;
  }

  public ArrayList<Entry> getEntries() {
    return entries;
  }

  public void appendEntries(ArrayList<Entry> entries) {
    this.entries.addAll(entries);
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
    for (Entry e : entries) sj.add(e.toString());
    return sj.toString();
  }
}
