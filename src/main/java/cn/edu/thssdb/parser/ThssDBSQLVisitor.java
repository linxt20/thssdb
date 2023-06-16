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
import cn.edu.thssdb.schema.Database;
import cn.edu.thssdb.schema.Manager;
import cn.edu.thssdb.schema.Table;
import cn.edu.thssdb.sql.SQLBaseVisitor;
import cn.edu.thssdb.sql.SQLParser;
import cn.edu.thssdb.type.*;
import cn.edu.thssdb.utils.Global;
import cn.edu.thssdb.utils.Pair;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;

import static cn.edu.thssdb.utils.Global.storage_dir;

public class ThssDBSQLVisitor extends SQLBaseVisitor<LogicalPlan> {

  private long sessionId;

  public ThssDBSQLVisitor(long sessionId) {
    super();
    this.sessionId = sessionId;
  }

  @Override
  public LogicalPlan visitBeginTransactionStmt(SQLParser.BeginTransactionStmtContext ctx) {
    try {
      if (!Manager.getInstance().transaction_sessions.contains(sessionId)) {
        Manager.getInstance().transaction_sessions.add(sessionId);
        ArrayList<String> s_lock_tables = new ArrayList<>();
        ArrayList<String> x_lock_tables = new ArrayList<>();
        Manager.getInstance().s_lock_dict.put(sessionId, s_lock_tables);
        Manager.getInstance().x_lock_dict.put(sessionId, x_lock_tables);
      } else {
        System.out.println("session already in a transaction.");
      }
    } catch (Exception e) {
      throw new RuntimeException(e.getMessage());
    }
    return new BeginTransactionPlan();
  }

  @Override
  public LogicalPlan visitAutoBeginTransactionStmt(SQLParser.AutoBeginTransactionStmtContext ctx) {
    try {
      if (!Manager.getInstance().transaction_sessions.contains(sessionId)) {
        Manager.getInstance().transaction_sessions.add(sessionId);
        ArrayList<String> s_lock_tables = new ArrayList<>();
        ArrayList<String> x_lock_tables = new ArrayList<>();
        Manager.getInstance().s_lock_dict.put(sessionId, s_lock_tables);
        Manager.getInstance().x_lock_dict.put(sessionId, x_lock_tables);
      } else {
        System.out.println("session already in a transaction.");
      }
    } catch (Exception e) {
      throw new RuntimeException(e.getMessage());
    }
    return new AutoBeginTransactionPlan();
  }

  @Override
  public LogicalPlan visitCommitStmt(SQLParser.CommitStmtContext ctx) {
    try {
      if (Manager.getInstance().transaction_sessions.contains(sessionId)) {
        Database the_database = Manager.getInstance().getCurrentDB();
        String db_name = the_database.getName();
        Manager.getInstance().transaction_sessions.remove(sessionId);
        ArrayList<String> table_x_list = Manager.getInstance().x_lock_dict.get(sessionId);
        for (String table_name : table_x_list) {
          Table the_table = the_database.get(table_name);
          the_table.free_x_lock(sessionId);
          the_table.quit_tran();
        }
        table_x_list.clear();
        Manager.getInstance().x_lock_dict.put(sessionId, table_x_list);
        if (Global.DATABASE_ISOLATION_LEVEL == Global.ISOLATION_LEVEL.SERIALIZABLE) {
          ArrayList<String> table_s_list = Manager.getInstance().s_lock_dict.get(sessionId);
          for (String table_name : table_s_list) {
            Table the_table = the_database.get(table_name);
            the_table.free_s_lock(sessionId);
            the_table.quit_tran();
          }
          table_s_list.clear();
          Manager.getInstance().s_lock_dict.put(sessionId, table_s_list);
        }

        String log_name = storage_dir + db_name + ".log";
        File file = new File(log_name);
        if (file.exists() && file.isFile() && file.length() > 50000) {
          System.out.println("Clear database log");
          try {
            FileWriter writer = new FileWriter(log_name);
            writer.write("");
            writer.close();
          } catch (IOException e) {
            e.printStackTrace();
          }
          Manager.getInstance().persist_database(db_name);
        }
      } else {
        System.out.println("session not in a transaction.");
      }
    } catch (Exception e) {
      throw new RuntimeException(e.getMessage());
    }
    return new CommitPlan();
  }

  @Override
  public LogicalPlan visitAutoCommitStmt(SQLParser.AutoCommitStmtContext ctx) {
    try {
      if (Manager.getInstance().transaction_sessions.contains(sessionId)) {
        Database the_database = Manager.getInstance().getCurrentDB();
        Manager.getInstance().transaction_sessions.remove(sessionId);
        ArrayList<String> table_list = Manager.getInstance().x_lock_dict.get(sessionId);
        for (String table_name : table_list) {
          Table the_table = the_database.get(table_name);
          the_table.free_x_lock(sessionId);
          the_table.quit_tran();
        }
        table_list.clear();
        Manager.getInstance().x_lock_dict.put(sessionId, table_list);

        if (Global.DATABASE_ISOLATION_LEVEL == Global.ISOLATION_LEVEL.SERIALIZABLE) {
          ArrayList<String> table_s_list = Manager.getInstance().s_lock_dict.get(sessionId);
          for (String table_name : table_s_list) {
            Table the_table = the_database.get(table_name);
            the_table.free_s_lock(sessionId);
            the_table.quit_tran();
          }
          table_s_list.clear();
          Manager.getInstance().s_lock_dict.put(sessionId, table_s_list);
        }

      } else {
        System.out.println("session not in a transaction.");
      }
    } catch (Exception e) {
      throw new RuntimeException(e.getMessage());
    }
    return new AutoCommitPlan();
  }

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
  public LogicalPlan visitShowDbStmt(SQLParser.ShowDbStmtContext ctx) {
    return new ShowDatabasesPlan();
  }

  @Override
  public LogicalPlan visitShowDbMetaStmt(SQLParser.ShowDbMetaStmtContext ctx) {
    return new ShowDatabaseMetaPlan(ctx.databaseName().getText());
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
    Column[] columns = new Column[n];
    int i = 0;

    for (SQLParser.ColumnDefContext // 读取各个列的定义
        columnContext : ctx.columnDef()) {
      columns[i++] = readColumn(columnContext);
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
      // 判断primary key是否存在
      for (String compositeName : compositeNames) {
        boolean found = false;
        for (Column c : columns) {
          if (c.getName().toLowerCase().equals(compositeName)) {
            c.setPrimary(1);
            found = true;
          }
        }
        if (!found) {
          throw new RuntimeException("Error: primary key " + compositeName + " not found");
        }
      }
    }
    return new CreateTablePlan(name, columns);
  }

  /** 描述：处理更新元素 */
  public LogicalPlan visitUpdateStmt(SQLParser.UpdateStmtContext ctx) {
    System.out.println("visitUpdateStmt");
    String table_name = ctx.tableName().getText().toLowerCase();
    String column_name = ctx.columnName().getText().toLowerCase();
    Comparer value = visit_expression(ctx.expression());
    if (ctx.K_WHERE() == null) {
      //transaction_wait_write(table_name);
      return new UpdatePlan(table_name, column_name, value, null);
    }
    Logic logic = visitMultiple_condition(ctx.multipleCondition());
    //transaction_wait_write(table_name);
    return new UpdatePlan(table_name, column_name, value, logic);
  }

  /** 描述：本应该是得到算术表达式，但是因为没有实现算术表达式，所以直接返回数值 */
  public Comparer visit_expression(SQLParser.ExpressionContext ctx) {
    if (ctx.comparer() != null) {
      if (ctx.comparer().columnFullName() != null) {
        return new Comparer(ComparerType.COLUMN, ctx.comparer().columnFullName().getText());
      }
      // 获得类型和内容
      LiteralType type = visitLiteral_value(ctx.comparer().literalValue());
      String text = ctx.comparer().literalValue().getText();
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
    } else return null;
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
  public Condition visit_Condition(SQLParser.ConditionContext ctx) {
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
    if (ctx.condition() != null) return new Logic(visit_Condition(ctx.condition()));

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
      // 如果使用了join，查找的元素前面需要加上表名。
      if (ctx.tableQuery(0).K_JOIN().size() != 0 && !columnName.contains(".")) {
        throw new RuntimeException(
            "Error: in joint query, column name should be like {table.column}");
      }
      columnsSelected[i] = columnName;
    }

    // 获取from的table，建立querytable
    int query_count = ctx.tableQuery().size();
    if (query_count == 0) {
      throw new RuntimeException("Error: no selected table");
    }
    ArrayList<String> table_names = new ArrayList<>();
    Logic logicForJoin = null;
    try {
      if (ctx.tableQuery(0).K_JOIN().size() == 0) {
        table_names.add(ctx.tableQuery(0).tableName(0).getText().toLowerCase());
      }
      // 如果是复合表，需要读取join逻辑
      else {
        logicForJoin = visitMultiple_condition(ctx.tableQuery(0).multipleCondition());
        for (SQLParser.TableNameContext subCtx : ctx.tableQuery(0).tableName()) {
          table_names.add(subCtx.getText().toLowerCase());
        }
      }
    } catch (Exception e) {
      throw new RuntimeException("Error: no selected table");
    }
    // 建立逻辑，获得结果
    Logic logic = null;
    if (ctx.K_WHERE() != null) logic = visitMultiple_condition(ctx.multipleCondition());

    transaction_wait_read(table_names);
    return new SelectPlan(table_names, columnsSelected, logicForJoin, logic, distinct);
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
    Pair<ColumnType, Integer> type = visitType_Name(ctx.typeName());
    ColumnType columnType = type.getKey();
    int maxLength = type.getValue(); // 这里的maxLength对于int，long，float，double都是-1，只有string是最大长度
    return new Column(name, columnType, primary, not_null, maxLength);
  }

  /** 描述：处理创建列时的type，max length */
  public Pair<ColumnType, Integer> visitType_Name(SQLParser.TypeNameContext ctx) {
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
        throw new RuntimeException("Error: ValueFormatException");
      }
    }
    return null;
  }

  @Override
  public LogicalPlan visitInsertStmt(SQLParser.InsertStmtContext ctx) {
    // table name
    String table_name = ctx.tableName().getText().toLowerCase();
    // column name 转换为string
    String[] column_names = null;
    if (ctx.columnName() != null && ctx.columnName().size() != 0) {
      column_names = new String[ctx.columnName().size()];
      for (int i = 0; i < ctx.columnName().size(); i++)
        column_names[i] = ctx.columnName(i).getText().toLowerCase();
    }
    // 应受隔离级别限制，暂时不实现
    System.out.println("[Debug] valueEntry" + ctx.valueEntry().toString());
    transaction_wait_write(table_name);
    return new InsertPlan(table_name, column_names, ctx.valueEntry());
  }

  public void transaction_wait_read(ArrayList<String> table_names) {
    if (Manager.getInstance().transaction_sessions.contains(sessionId)) {
      while (true) {
        if (!Manager.getInstance().session_queue.contains(sessionId)) // 新加入一个session
        {
          ArrayList<Integer> lock_result = new ArrayList<>();
          for (String name : table_names) {
            Table the_table = Manager.getInstance().getCurrentDB().get(name);
            int get_lock = the_table.get_s_lock(sessionId);
            lock_result.add(get_lock);
          }
          if (lock_result.contains(-1)) {
            for (String table_name : table_names) {
              Table the_table = Manager.getInstance().getCurrentDB().get(table_name);
              the_table.free_s_lock(sessionId);
            }
            Manager.getInstance().session_queue.add(sessionId);
          } else {
            ArrayList<String> tmp = Manager.getInstance().s_lock_dict.get(sessionId);
            for (String table_name : table_names) {
              tmp.add(table_name);
            }
            Manager.getInstance().s_lock_dict.put(sessionId, tmp);
            break;
          }
        } else // 之前等待的session
        {
          if (Manager.getInstance().session_queue.get(0) == sessionId) // 只查看阻塞队列开头session
          {
            ArrayList<Integer> lock_result = new ArrayList<>();
            for (String name : table_names) {
              Table the_table = Manager.getInstance().getCurrentDB().get(name);
              int get_lock = the_table.get_s_lock(sessionId);
              lock_result.add(get_lock);
            }
            if (!lock_result.contains(-1)) {
              ArrayList<String> tmp = Manager.getInstance().s_lock_dict.get(sessionId);
              for (String table_name : table_names) {
                tmp.add(table_name);
              }
              Manager.getInstance().s_lock_dict.put(sessionId, tmp);
              Manager.getInstance().session_queue.remove(0);
              break;
            } else {
              for (String table_name : table_names) {
                Table the_table = Manager.getInstance().getCurrentDB().get(table_name);
                the_table.free_s_lock(sessionId);
              }
            }
          }
        }
        try {
          Thread.sleep(500); // 休眠3秒
        } catch (Exception e) {
          System.out.println("Got an exception!");
        }
      }
    }
  }

  public void transaction_wait_write(String table_name) {
    if (Manager.getInstance().transaction_sessions.contains(sessionId)) {
      Table the_table = Manager.getInstance().getCurrentDB().get(table_name);
      while (true) {
        if (!Manager.getInstance().session_queue.contains(sessionId)) // 新加入一个session
        {
          int get_lock = the_table.get_x_lock(sessionId);
          if (get_lock != -1) {
            if (get_lock == 1) {
              ArrayList<String> tmp = Manager.getInstance().x_lock_dict.get(sessionId);
              tmp.add(table_name);
              Manager.getInstance().x_lock_dict.put(sessionId, tmp);
            }
            break;
          } else {
            Manager.getInstance().session_queue.add(sessionId);
          }
        } else // 之前等待的session
        {
          if (Manager.getInstance().session_queue.get(0) == sessionId) // 只查看阻塞队列开头session
          {
            int get_lock = the_table.get_x_lock(sessionId);
            if (get_lock != -1) {
              if (get_lock == 1) {
                ArrayList<String> tmp = Manager.getInstance().x_lock_dict.get(sessionId);
                tmp.add(table_name);
                Manager.getInstance().x_lock_dict.put(sessionId, tmp);
              }
              Manager.getInstance().session_queue.remove(0);
              break;
            }
          }
        }
        try {
          Thread.sleep(500); // 休眠3秒
        } catch (Exception e) {
          System.out.println("Got an exception!");
        }
      }
    }
  }

  @Override
  public LogicalPlan visitDeleteStmt(SQLParser.DeleteStmtContext ctx) {
    String table_name = ctx.tableName().getText().toLowerCase();
    if (ctx.K_WHERE() == null) {
      try {
        //transaction_wait_write(table_name);
        return new DeletePlan(table_name, null);
      } catch (Exception e) {
        System.out.println(e.getMessage());
      }
    }
    Logic logic = visitMultiple_condition(ctx.multipleCondition());
    //transaction_wait_write(table_name);
    return new DeletePlan(table_name, logic);
  }
  // TODO: parser to more logical plan
}
