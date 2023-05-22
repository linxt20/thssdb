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
import cn.edu.thssdb.schema.Column;
import cn.edu.thssdb.sql.SQLBaseVisitor;
import cn.edu.thssdb.sql.SQLParser;
import cn.edu.thssdb.type.ColumnType;
import cn.edu.thssdb.utils.Pair;

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
      String[] compositeNames = new String[tableConstraintSize];// visitTableConstraint1(ctx.tableConstraint());
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

  // TODO: parser to more logical plan
}
