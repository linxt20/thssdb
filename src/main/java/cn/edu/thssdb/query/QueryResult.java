package cn.edu.thssdb.query;

import cn.edu.thssdb.schema.Entry;
import cn.edu.thssdb.schema.Row;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

/** 描述：用于生成查询结果，包括对结果按照列进行选择，包括对distinct进行判重处理 参数：对应的querytable，选中的列，是否distinct */
public class QueryResult {
  private QueryTable mTable;
  private ArrayList<MetaInfo> mMetaInfoList;
  private boolean mWhetherDistinct;
  private HashSet<String> mHashSet = null; // 用于distinct语句
  public boolean mWhetherRight;
  public ArrayList<Integer> mColumnIndex;
  public HashMap<String, Integer> mColumnIndexMap = null;
  public List<String> mColumnName;
  public ArrayList<Row> mResultList;

  // 正常构造函数
  public QueryResult(QueryTable queryTable, String[] selectColumns, boolean whetherDistinct) {
    this.mTable = queryTable;
    this.mWhetherDistinct = whetherDistinct;
    this.mHashSet = new HashSet<>();
    mWhetherRight = true;
    this.mMetaInfoList = new ArrayList<MetaInfo>();
    this.mMetaInfoList.addAll(queryTable.GenerateMetaInfo());
    this.mResultList = new ArrayList<Row>();
    InitColumns(selectColumns);
  }

  /** 描述：按照传入时候选中的列，初始化queryresult的各列信息 参数：选中的列 返回：无 */
  private void InitColumns(String[] selectColumns) {
    this.mColumnIndex = new ArrayList<>();
    this.mColumnName = new ArrayList<>();

    // 选中了一些列
    if (selectColumns != null) {
      for (String column_name : selectColumns) {
        this.mColumnIndex.add(GetColumnIndex(column_name));
        this.mColumnName.add(column_name);
      }
    } else {
      throw new RuntimeException("You haven't selected any column!");
    }
    // 没有选中任何列，那就全部返回 ？？这是在干什么
    //    else {
    //      int offset = 0, joinjudge = 1;
    //      if (mMetaInfoList.size() == 1) {
    //        joinjudge = 0;
    //      }
    //      for (MetaInfo metaInfo : mMetaInfoList) {
    //        for (int i = 0; i < metaInfo.GetColumnSize(); i++) {
    //          String name;
    //          if (joinjudge == 1) {
    //            name = metaInfo.GetFullName(i);
    //          } else {
    //            name = metaInfo.columns.get(i).getName();
    //          }
    //          this.mColumnIndex.add(offset + i);
    //          this.mColumnName.add(name);
    //        }
    //        offset += metaInfo.GetColumnSize();
    //      }
    //    }
  }

  /** 描述：找到一个列名在index对应的位置，这里位置指的是在合并后的row中的位置 参数：列名 返回：位置 */
  public int GetColumnIndex(String column_name) {
    int index = 0;
    // 如果只有columnname，则说明不是join的table，直接找就行了
    if (!column_name.contains(".")) {
      index = mMetaInfoList.get(0).ColumnFind(column_name);
      if (index < 0) {
        throw new RuntimeException("AttributeNotFoundException" + column_name);
      }
    }
    // 如果是tablename.columnname这样的（join操作要求必须使用这种形式）
    else {
      // 则建立一个HashMap，key是columnname，value是这个table在new_row中的index
      if (mColumnIndexMap == null) {
        mColumnIndexMap = new HashMap<>();
        int offset = 0;
        for (MetaInfo metaInfo : mMetaInfoList) {
          for (int i = 0; i < metaInfo.GetColumnSize(); i++) {
            String name = metaInfo.GetFullName(i);
            mColumnIndexMap.put(name, offset + i);
          }
          offset += metaInfo.GetColumnSize();
        }
      }
      if (mColumnIndexMap.containsKey(column_name)) {
        index = mColumnIndexMap.get(column_name);
      } else {
        throw new RuntimeException("AttributeNotFoundException" + column_name);
      }
    }
    return index;
  }

  /** 描述：获取所有搜索结果 参数：无 返回：所有搜索结果，返回的每个row都是和mColumnName一一对应的，如果distinct还会用哈希判重 */
  public void GenerateQueryRecords() {
    while (mTable.hasNext()) {
      QueryRow new_row = mTable.next();
      if (new_row == null) {
        break;
      }
      Entry[] entries = new Entry[mColumnIndex.size()];
      ArrayList<Entry> full_entries = new_row.getEntries();
      for (int i = 0; i < mColumnIndex.size(); i++) {
        int index = mColumnIndex.get(i);
        entries[i] = full_entries.get(index);
      }
      Row the_row = new Row(entries);
      String row_string = the_row.toString();
      if (!mWhetherDistinct || !mHashSet.contains(row_string)) {
        mResultList.add(the_row);
        if (mWhetherDistinct) {
          mHashSet.add(row_string);
        }
      }
    }
  }
}
