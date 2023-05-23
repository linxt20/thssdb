/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package cn.edu.thssdb.parser;

import cn.edu.thssdb.plan.LogicalPlan;
import cn.edu.thssdb.plan.impl.*;
import cn.edu.thssdb.query.*;
import cn.edu.thssdb.schema.Column;
import cn.edu.thssdb.sql.SQLBaseVisitor;
import cn.edu.thssdb.sql.SQLParser;
import cn.edu.thssdb.type.ColumnType;
import cn.edu.thssdb.utils.Pair;

import java.util.ArrayList;

public class ThssDBSQLVisitor extends SQLBaseVisitor<LogicalPlan> {

  @Override
  public LogicalPlan visitCreateDbStmt(SQLParser.CreateDbStmtContext ctx) {
    return new CreateDatabasePlan(ctx.databaseName().getText());
  }

  @Override
  public LogicalPlan visitDropDbStmt(SQLParser.DropDbStmtContext ctx) {
    return new DropDatabasePlan(ctx.databaseName().getText());
  }

  @Override
  public LogicalPlan visitUseDbStmt(SQLParser.UseDbStmtContext ctx) {
    return new UseDatabasePlan(ctx.databaseName().getText());
  }

  @Override
  public LogicalPlan visitShowTableStmt(SQLParser.ShowTableStmtContext ctx) {
    return new ShowTablePlan(ctx.tableName().getText());
  }

  @Override
  public LogicalPlan visitDropTableStmt(SQLParser.DropTableStmtContext ctx) {
    return new DropTablePlan(ctx.tableName().getText());
  }

  @Override
  public LogicalPlan visitCreateTableStmt(SQLParser.CreateTableStmtContext ctx) {
    // TODO 需要修改
    String name = ctx.tableName().getText();
    int n = ctx.columnDef().size();
    System.out.println("1 visitCreateTableStmt column size: " + n + " name: " + name);
    Column[] columns = new Column[n];
    int i = 0;

    // 读取各个列的定义
    for (SQLParser.ColumnDefContext columnContext : ctx.columnDef()) {
      columns[i++] = readColumn(columnContext);
      System.out.println(
          "  visitCreateTableStmt: " + columns[i - 1].getName() + " " + columns[i - 1].getType());
    }

    // 读取表定义末端的信息--primary key
    if (ctx.tableConstraint() != null) {
      // 处理表定义的限制，即create table中最后一行指定的primary key
      int tableConstraintSize = ctx.tableConstraint().columnName().size();
      String[] compositeNames =
          new String[tableConstraintSize]; // visitTableConstraint1(ctx.tableConstraint());
      for (int j = 0; j < tableConstraintSize; j++) {
        compositeNames[j] = ctx.tableConstraint().columnName(j).getText().toLowerCase();
      }

      for (String compositeName : compositeNames) {
        boolean found = false;
        for (Column c : columns) {
          if (c.getName().toLowerCase().equals(compositeName.toLowerCase())) {
            c.setPrimary(1);
            found = true;
          }
        }
        if (!found) {
          System.out.println("Error: primary key " + compositeName + " not found");
          // throw new AttributeNotFoundException(compositeName);
        }
      }
    }
    // System.out.println("Before Return visitCreateTableStmt: " + columns[0].getName() + " " +
    // columns[0].getType());
    return new CreateTablePlan(name, columns);
  }

  /** 描述：本应该是得到算术表达式，但是因为没有实现算术表达式，所以直接返回数值 */
  public Comparer visit_expression(SQLParser.ExpressionContext ctx) {
    if (ctx.comparer() != null) return visit_comparer(ctx.comparer());
    else return null;
  }

  /** 描述：读取一个comparer，值+类型 */
  public Comparer visit_comparer(SQLParser.ComparerContext ctx) {
    // 处理column情况
    if (ctx.columnFullName() != null) {
      return new Comparer(ComparerType.COLUMN, ctx.columnFullName().getText());
    }
    // 获得类型和内容
    LiteralType type = visitLiteral_value(ctx.literalValue());
    String text = ctx.literalValue().getText();
    switch (type) {
      case NUMBER:
        return new Comparer(ComparerType.NUMBER, text);
      case STRING:
        return new Comparer(ComparerType.STRING, text.substring(1, text.length() - 1));
      case NULL:
        return new Comparer(ComparerType.NULL, null);
      default:
        return null;
    }
  }

  /** 描述：获取单一数值的类型 */
  public LiteralType visitLiteral_value(SQLParser.LiteralValueContext ctx) {
    if (ctx.NUMERIC_LITERAL() != null) {
      return LiteralType.NUMBER;
    }
    if (ctx.STRING_LITERAL() != null) {
      return LiteralType.STRING;
    }
    if (ctx.K_NULL() != null) {
      return LiteralType.NULL;
    }
    return null;
  }

  /** 描述：处理逻辑建立 */
  public Condition visitCondition1(SQLParser.ConditionContext ctx) {
    Comparer left = visit_expression(ctx.expression(0));
    Comparer right = visit_expression(ctx.expression(1));
    ConditionType type = null; // visitComparator(ctx.comparator());
    if (ctx.comparator().EQ() != null) {
      type = ConditionType.EQ;
    } else if (ctx.comparator().NE() != null) {
      type = ConditionType.NE;
    } else if (ctx.comparator().GT() != null) {
      type = ConditionType.GT;
    } else if (ctx.comparator().LT() != null) {
      type = ConditionType.LT;
    } else if (ctx.comparator().GE() != null) {
      type = ConditionType.GE;
    } else if (ctx.comparator().LE() != null) {
      type = ConditionType.LE;
    }
    return new Condition(left, right, type);
  }

  /** 描述：处理复合逻辑 */
  public Logic visitMultiple_condition(SQLParser.MultipleConditionContext ctx) {
    // 单一条件
    Object a = ctx.multipleCondition(0);
    Object b = ctx.AND();
    if (ctx.condition() != null) return new Logic(visitCondition1(ctx.condition()));

    // 复合逻辑
    LogicType logic_type;
    if (ctx.AND() != null) {
      logic_type = LogicType.AND;
    } else {
      logic_type = LogicType.OR;
    }
    return new Logic(
        visitMultiple_condition(ctx.multipleCondition(0)),
        visitMultiple_condition(ctx.multipleCondition(1)),
        logic_type);
  }

  /** 描述：处理查询元素 */
  @Override
  public LogicalPlan visitSelectStmt(SQLParser.SelectStmtContext ctx) {
    System.out.println("visitSelectStmt");
    boolean distinct = false;
    if (ctx.K_DISTINCT() != null) distinct = true;
    int columnSize = ctx.resultColumn().size();
    String[] columnsSelected = new String[columnSize];
    // 获取select的列名
    for (int i = 0; i < columnSize; i++) {
      String columnName = ctx.resultColumn(i).getText().toLowerCase();
      if (columnName.equals("*")) {
        // 如果输入的是select *，则将columnsSelected置为null  g4文件能够接受非字母输入吗？？？
        columnsSelected = null;
        break;
      }
      columnsSelected[i] = columnName;
    }

    // 获取from的table，建立querytable
    int query_count = ctx.tableQuery().size();
    if (query_count == 0) {
      // throw new NoSelectedTableException();
      System.out.println("Error: no selected table");
    }
    ArrayList<String> table_names = new ArrayList<>();
    Logic logicForJoin = null;
    try {
      // System.out.println("table names: " + ctx.tableQuery());
      // 如果没有join，即为单一表
      if (ctx.tableQuery(0).K_JOIN().size() == 0) {
        table_names.add(ctx.tableQuery(0).tableName(0).getText().toLowerCase());
        // the_query_table =
        // the_database.BuildSingleQueryTable(ctx.tableQuery(0).table_name(0).getText().toLowerCase());
      }
      // 如果是复合表，需要读取join逻辑
      else {
        logicForJoin = visitMultiple_condition(ctx.tableQuery(0).multipleCondition());
        for (SQLParser.TableNameContext subCtx : ctx.tableQuery(0).tableName()) {
          table_names.add(subCtx.getText().toLowerCase());
        }
        // the_query_table = the_database.BuildJointQueryTable(table_names, logicForJoin);
      }
    } catch (Exception e) {
      // QueryResult error_result = new QueryResult(e.toString());
      // return error_result;
    }
    // 建立逻辑，获得结果
    Logic logic = null;
    if (ctx.K_WHERE() != null) logic = visitMultiple_condition(ctx.multipleCondition());
    /*
    if(manager.transaction_sessions.contains(session))
    {
      //manager.session_queue.add(session);
      while(true)
      {
        if(!manager.session_queue.contains(session))   //新加入一个session
        {
          ArrayList<Integer> lock_result = new ArrayList<>();
          for (String name : table_names) {
            Table the_table = the_database.get(name);
            int get_lock = the_table.get_s_lock(session);
            lock_result.add(get_lock);
          }
          if(lock_result.contains(-1))
          {
            for (String table_name : table_names) {
              Table the_table = the_database.get(table_name);
              the_table.free_s_lock(session);
            }
            manager.session_queue.add(session);

          }else
          {
            break;
          }
        }else    //之前等待的session
        {
          if(manager.session_queue.get(0)==session)  //只查看阻塞队列开头session
          {
            ArrayList<Integer> lock_result = new ArrayList<>();
            for (String name : table_names) {
              Table the_table = the_database.get(name);
              int get_lock = the_table.get_s_lock(session);
              lock_result.add(get_lock);
            }
            if(!lock_result.contains(-1))
            {
              manager.session_queue.remove(0);
              break;
            }else
            {
              for (String table_name : table_names) {
                Table the_table = the_database.get(table_name);
                the_table.free_s_lock(session);
              }
            }
          }
        }
        try
        {
          //System.out.print("session: "+session+": ");
          //System.out.println(manager.session_queue);
          Thread.sleep(500);   // 休眠3秒
        } catch (Exception e) {
          System.out.println("Got an exception!");
        }
      }
      try {
        for (String table_name : table_names) {
          Table the_table = the_database.get(table_name);
          the_table.free_s_lock(session);
        }
        QueryResult result = the_database.select(columnsSelected, the_query_table, logic, distinct);
        return result;
      } catch (Exception e) {
        QueryResult error_result = new QueryResult(e.toString());
        return error_result;
      }
    }*/

    // else
    // {
    return new SelectPlan(table_names, columnsSelected, logicForJoin, logic, distinct);
    //      try {
    //        return new SelectPlan(columnsSelected, the_query_table, logic, distinct);
    //      } catch (Exception e) {
    //        QueryResult error_result = new QueryResult(e.toString());
    //        return error_result;
    //      }
    // }
  }

  // 描述：读取列定义中的信息---名字，类型，是否主键，是否非空，最大长度
  public Column readColumn(SQLParser.ColumnDefContext ctx) {
    // 读取当前列是否primary， not null
    boolean not_null = false;
    int primary = 0;
    for (SQLParser.ColumnConstraintContext subCtx : ctx.columnConstraint()) {
      if (subCtx.K_PRIMARY() != null) {
        primary = 1;
        not_null = true;
      }
      if (subCtx.K_NULL() != null) {
        not_null = true;
      }
    }
    // 获得名称和类型和最大长度
    String name = ctx.columnName().getText().toLowerCase();
    Pair<ColumnType, Integer> type = visitTypeName1(ctx.typeName());
    ColumnType columnType = type.getKey();
    int maxLength = type.getValue(); // 这里的maxLength对于int，long，float，double都是-1，只有string是最大长度
    return new Column(name, columnType, primary, not_null, maxLength);
  }

  /** 描述：处理创建列时的type，max length */
  public Pair<ColumnType, Integer> visitTypeName1(SQLParser.TypeNameContext ctx) {
    if (ctx.T_INT() != null) {
      return new Pair<>(ColumnType.INT, -1);
    }
    if (ctx.T_LONG() != null) {
      return new Pair<>(ColumnType.LONG, -1);
    }
    if (ctx.T_FLOAT() != null) {
      return new Pair<>(ColumnType.FLOAT, -1);
    }
    if (ctx.T_DOUBLE() != null) {
      return new Pair<>(ColumnType.DOUBLE, -1);
    }
    if (ctx.T_STRING() != null) {
      try {
        // 仅string返回值和最大长度
        return new Pair<>(ColumnType.STRING, Integer.parseInt(ctx.NUMERIC_LITERAL().getText()));
      } catch (Exception e) {
        System.out.println("Error: ValueFormatException");
        // throw new ValueFormatException();
      }
    }
    return null;
  }

  @Override
  public LogicalPlan visitInsertStmt(SQLParser.InsertStmtContext ctx) {
    // table name
    String table_name = ctx.tableName().getText().toLowerCase();

    // column name 转换为string TODO 有效率更高的转换方式吗？强制类型转换？
    String[] column_names = null;
    if (ctx.columnName() != null && ctx.columnName().size() != 0) {
      column_names = new String[ctx.columnName().size()];
      for (int i = 0; i < ctx.columnName().size(); i++)
        column_names[i] = ctx.columnName(i).getText().toLowerCase();
    }
    // 应受隔离级别限制，暂时不实现
    System.out.println("[Debug] valueEntry" + ctx.valueEntry().toString());
    return new InsertPlan(table_name, column_names, ctx.valueEntry());
    // TODO 先处理数据，处理不放在并发控制里
    //    for (SQLParser.ValueEntryContext subCtx : ctx.valueEntry())
    //    {
    //      String[] values = visitValue_entry6(subCtx);
    //      try {
    //        the_database.insert(table_name, column_names, values);
    //      } catch (Exception e) {
    //        return e.toString();
    //      }
    //    }

    // TODO 并发控制,session设定事务的隔离级别 之后移到IServiceHandler中
    //    Database the_database = GetCurrentDB();
    //
    //    if(manager.transaction_sessions.contains(session))
    //    {
    //      //manager.session_queue.add(session);
    //      Table the_table = the_database.get(table_name);
    //      while(true)
    //      {
    //        if(!manager.session_queue.contains(session))   //新加入一个session
    //        {
    //          int get_lock = the_table.get_x_lock(session);
    //          if(get_lock!=-1)
    //          {
    //            if(get_lock==1)
    //            {
    //              ArrayList<String> tmp = manager.x_lock_dict.get(session);
    //              tmp.add(table_name);
    //              manager.x_lock_dict.put(session,tmp);
    //            }
    //            break;
    //          }else
    //          {
    //            manager.session_queue.add(session);
    //          }
    //        }else    //之前等待的session
    //        {
    //          if(manager.session_queue.get(0)==session)  //只查看阻塞队列开头session
    //          {
    //            int get_lock = the_table.get_x_lock(session);
    //            if(get_lock!=-1)
    //            {
    //              if(get_lock==1)
    //              {
    //                ArrayList<String> tmp = manager.x_lock_dict.get(session);
    //                tmp.add(table_name);
    //                manager.x_lock_dict.put(session,tmp);
    //              }
    //              manager.session_queue.remove(0);
    //              break;
    //            }
    //          }
    //        }
    //        try
    //        {
    //          //System.out.print("session: "+session+": ");
    //          //System.out.println(manager.session_queue);
    //          Thread.sleep(500);   // 休眠3秒
    //        } catch (Exception e) {
    //          System.out.println("Got an exception!");
    //        }
    //      }
    //      for (SQLParser.ValueEntryContext subCtx : ctx.valueEntry())
    //      {
    //        String[] values = visitValue_entry6(subCtx);
    //        try {
    //          if(column_names == null)
    //          {
    //            the_table.insert(values);
    //          }
    //          else
    //          {
    //            the_table.insert(column_names, values);
    //          }
    //        } catch (Exception e) {
    //          return e.toString();
    //        }
    //      }
    //    }else{
    //      for (SQLParser.ValueEntryContext subCtx : ctx.valueEntry())
    //      {
    //        String[] values = visitValue_entry6(subCtx);
    //        try {
    //          the_database.insert(table_name, column_names, values);
    //        } catch (Exception e) {
    //          return e.toString();
    //        }
    //      }
    //    }

  }

  @Override
  public LogicalPlan visitDeleteStmt(SQLParser.DeleteStmtContext ctx) {
    String table_name = ctx.tableName().getText().toLowerCase();
    // TODO null的处理
    if (ctx.K_WHERE() == null) {
      try {
        return new DeletePlan(table_name, null);
      } catch (Exception e) {
        System.out.println(e.getMessage());
      }
    }
    Logic logic = visitMultiple_condition(ctx.multipleCondition());
    return new DeletePlan(table_name, logic);
    // TODO 并发控制
    //    if(manager.transaction_sessions.contains(session))
    //    {
    //      //manager.session_queue.add(session);
    //      Table the_table = the_database.get(table_name);
    //      while(true)
    //      {
    //        if(!manager.session_queue.contains(session))   //新加入一个session
    //        {
    //          int get_lock = the_table.get_x_lock(session);
    //          if(get_lock!=-1)
    //          {
    //            if(get_lock==1)
    //            {
    //              ArrayList<String> tmp = manager.x_lock_dict.get(session);
    //              tmp.add(table_name);
    //              manager.x_lock_dict.put(session,tmp);
    //            }
    //            break;
    //          }else
    //          {
    //            manager.session_queue.add(session);
    //          }
    //        }else    //之前等待的session
    //        {
    //          if(manager.session_queue.get(0)==session)  //只查看阻塞队列开头session
    //          {
    //            int get_lock = the_table.get_x_lock(session);
    //            if(get_lock!=-1)
    //            {
    //              if(get_lock==1)
    //              {
    //                ArrayList<String> tmp = manager.x_lock_dict.get(session);
    //                tmp.add(table_name);
    //                manager.x_lock_dict.put(session,tmp);
    //              }
    //              manager.session_queue.remove(0);
    //              break;
    //            }
    //          }
    //        }
    //        try
    //        {
    //          //System.out.print("session: "+session+": ");
    //          //System.out.println(manager.session_queue);
    //          Thread.sleep(500);   // 休眠3秒
    //        } catch (Exception e) {
    //          System.out.println("Got an exception!");
    //        }
    //      }
    //
    //      try {
    //        return the_table.delete(logic);
    //      } catch (Exception e) {
    //        return e.toString();
    //      }
    //
    //    }
    //    else{
    //      try {
    //        return the_database.delete(table_name, logic);
    //      } catch (Exception e) {
    //        return e.toString();
    //      }
    //    }
  }

  // TODO: parser to more logical plan
}
