package cn.edu.thssdb.query;

import cn.edu.thssdb.schema.Row;
import cn.edu.thssdb.schema.Table;
import cn.edu.thssdb.schema.View;
import cn.edu.thssdb.type.ComparerType;
import cn.edu.thssdb.type.ConditionType;
import cn.edu.thssdb.type.ResultType;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;

/** 描述：多个table使用的联合查找表 参数：table */
public class MultiQueryTable extends QueryTable implements Iterator<Row> {

  private ArrayList<Iterator<Row>> mIterators; // 這裡是每個table的iterator
  private ArrayList<Table> mTables;
  Logic mLogicSelect; // 选择逻辑

  private int mType; // 0: no join，1: left join，2: right join 3: full join 4: 正常inner join
  private int whereIndex = -1; // where条件的列的index
  private Logic mLogicJoin; // 這裡是join的邏輯

  /** 长度=每个table，分别代表每个table要出来join的列 */
  private LinkedList<Row> mRowsToBeJoined; // 用于存放每个table要join的row

  public MultiQueryTable(ArrayList<Table> tables, Logic joinLogic, int joinType, Logic logicSelect) {
    super();
    this.mTables = tables;
    this.mIterators = new ArrayList<>();
    this.mRowsToBeJoined = new LinkedList<>();
    this.mLogicJoin = joinLogic;
    this.mColumns = new ArrayList<>();
    this.mType = joinType;
    this.mLogicSelect = logicSelect;
    String whereTableName ="";
//    if(mLogicSelect != null){
//      if(mLogicSelect.mTerminal){
//        System.out.println("mLogicSelect.mTerminal: " + mLogicSelect.mTerminal);
//        System.out.println("mLogicSelect.mCondition.mLeft.mType: " + mLogicSelect.mCondition.mLeft.mType.toString());
//        System.out.println("mLogicSelect.mCondition.mLeft.mValue: " + mLogicSelect.mCondition.mLeft.mValue.toString());
//      }
//    }
    if(mLogicSelect != null && mLogicSelect.mTerminal && mLogicSelect.mCondition.mLeft.mType == ComparerType.COLUMN){
      if(mLogicSelect.mCondition.mRight.mType != ComparerType.COLUMN){
        whereTableName = ((String)(mLogicSelect.mCondition.mLeft.mValue)).split("\\.")[0];
        //System.out.println("whereTableName: " + whereTableName);
      }
    }
    for (Table t : tables) {
      if(whereTableName.equals(t.tableName) && mLogicSelect.mCondition.mType == ConditionType.EQ){
        String columnName = ((String)(mLogicSelect.mCondition.mLeft.mValue)).split("\\.")[1];
        Comparable rightValue = mLogicSelect.mCondition.mRight.mValue;

        //System.out.println("columnName: " + columnName + " rightValue: " + rightValue.toString());
        View view = new View(t, columnName, rightValue);
        this.mColumns.addAll(view.columns);
        this.mIterators.add(view.iterator());
      }
      else{
        this.mColumns.addAll(t.columns);
        this.mIterators.add(t.iterator());
      }
    }
  }

  /** 描述：返回元数据信息 参数：无 返回：元数据信息 */
  @Override
  public ArrayList<MetaInfo> GenerateMetaInfo() {
    ArrayList<MetaInfo> the_meta = new ArrayList<>();
    for (Table table : mTables) {
      the_meta.add(new MetaInfo(table.tableName, table.columns));
    }
    return the_meta;
  }

  /** 描述：找到下一个符合条件的，放到队列里 参数：无 返回：无 */
  @Override
  public void PrepareNext() {
    while (true) {
      QueryRow the_row = JoinRows();
      if (the_row == null) {
        return;
      }
      // 这里mLogicJoin是join ... on...的那个on的逻辑，mLogicSelect是where的逻辑，两个同时满足才能加入队列
      if (mLogicJoin == null || mLogicJoin.GetResult(the_row) == ResultType.TRUE) {
        if (mLogicSelect == null || mLogicSelect.GetResult(the_row) == ResultType.TRUE) {
          mQueue.add(the_row);
          return;
        }
      }
    }
  }

  /** 描述：将下一组row连接成一个完整的row，用于判断 参数：无 返回：无 */
  private QueryRow JoinRows() {
    if (mRowsToBeJoined.isEmpty()) {
      // 遍历每个table的迭代器，将每个table的第一个row放入mRowsToBeJoined
      int i = 0;
      for (Iterator<Row> iter : mIterators) {
        if (!iter.hasNext()) {
          return null;
        }
        mRowsToBeJoined.push(iter.next());
        i++;
      }
      return new QueryRow(mRowsToBeJoined, mTables);
    } else {
      int index;
      for (index = mIterators.size() - 1; index >= 0; index--) {
        // 类似加法进位一样的机制：一直重新设置iterator，类似进位，直到有一个iterator有能进位的为止
        mRowsToBeJoined.pop();
        if (!mIterators.get(index).hasNext()) {
          mIterators.set(index, mTables.get(index).iterator());
        } else {
          break;
        }
      }
      if (index < 0) {
        return null;
      }
      // 再补回去
      for (int i = index; i < mIterators.size(); i++) {
        if (!mIterators.get(i).hasNext()) throw new RuntimeException("Iterator should have next");
        mRowsToBeJoined.push(mIterators.get(i).next());
      }
      return new QueryRow(mRowsToBeJoined, mTables);
    }
  }
}
