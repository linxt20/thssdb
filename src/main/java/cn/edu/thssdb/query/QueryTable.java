package cn.edu.thssdb.query;

import cn.edu.thssdb.schema.Column;
import cn.edu.thssdb.schema.Row;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;

/** 描述：querytable父类 构造函数：无 */
public abstract class QueryTable implements Iterator<Row> {
  LinkedList<QueryRow> mQueue; // 这个就是查询表的新结构
  boolean isFirst; // 是否是第一个元素
  public ArrayList<Column> mColumns; // 元数据信息

  public abstract void PrepareNext(); // 找到下一个符合条件的，放到队列里

  public abstract ArrayList<MetaInfo> GenerateMetaInfo(); // 返回元数据信息

  QueryTable() {
    this.mQueue = new LinkedList<>();
    this.isFirst = true;
  }

  /** 描述：判断是否还有元素 参数：无 返回：无 */
  @Override
  public boolean hasNext() {
    return isFirst || !mQueue.isEmpty();
  }

  /** 描述：返回下一个符合条件的元素，同时更新队列，保证除了自身之外非空 参数：无 返回：下一个元素row */
  @Override
  public QueryRow next() {
    if (mQueue.isEmpty()) {
      PrepareNext();
      if (isFirst) {
        isFirst = false;
      }
    }
    QueryRow result = null;
    if (!mQueue.isEmpty()) {
      result = mQueue.poll();
    } else {
      return null;
    }
    if (mQueue.isEmpty()) {
      PrepareNext();
    }
    return result;
  }
}
