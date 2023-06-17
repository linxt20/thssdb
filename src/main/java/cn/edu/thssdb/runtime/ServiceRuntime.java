package cn.edu.thssdb.runtime;

import cn.edu.thssdb.plan.LogicalGenerator;
import cn.edu.thssdb.plan.LogicalPlan;
import cn.edu.thssdb.plan.impl.*;
import cn.edu.thssdb.query.Comparer;
import cn.edu.thssdb.query.Logic;
import cn.edu.thssdb.query.QueryResult;
import cn.edu.thssdb.query.QueryTable;
import cn.edu.thssdb.rpc.thrift.ExecuteStatementResp;
import cn.edu.thssdb.schema.Column;
import cn.edu.thssdb.schema.Manager;
import cn.edu.thssdb.schema.Row;
import cn.edu.thssdb.schema.Table;
import cn.edu.thssdb.sql.SQLParser;
import cn.edu.thssdb.type.ColumnType;
import cn.edu.thssdb.utils.Global;
import cn.edu.thssdb.utils.StatusUtil;

import java.util.ArrayList;
import java.util.List;

public class ServiceRuntime {

  public static ExecuteStatementResp executeStatement(String statement, long sessionId) {
    statement = statement.trim();
    String cmd = statement.split("\\s+")[0];
    LogicalPlan plan;
    Boolean autoCommit = false;
    if ((cmd.toLowerCase().equals("insert")
            || cmd.toLowerCase().equals("update")
            || cmd.toLowerCase().equals("delete")
            || cmd.toLowerCase().equals("select"))
        && !Manager.getInstance().transaction_sessions.contains(sessionId)) {
      LogicalGenerator.generate("autobegin transaction", sessionId);
      plan = LogicalGenerator.generate(statement, sessionId); // 这里会调用parser解析语句
      System.out.println("=========================" + Manager.getInstance().toString());
      autoCommit = true;
    } else {
      plan = LogicalGenerator.generate(statement, sessionId); // 这里会调用parser解析语句
    }

    String name;
    switch (plan.getType()) {
      case CREATE_DB:
        System.out.println("IServiceHandler: [DEBUG] " + plan);
        CreateDatabasePlan createDatabasePlan = (CreateDatabasePlan) plan;
        name = createDatabasePlan.getDatabaseName();
        // 判断是否已经存在这个database，如果不存在则创建并返回success，否则返回fail
        if (Manager.getInstance().containDatabase(name)) {
          return new ExecuteStatementResp(StatusUtil.fail("Database already exists."), false);
        } else {
          Manager.getInstance().createDatabaseIfNotExists(name);
          return new ExecuteStatementResp(
              StatusUtil.success("Database " + name + " created."), false);
        }
      case DROP_DB:
        System.out.println("IServiceHandler: [DEBUG] " + plan);
        DropDatabasePlan dropDatabasePlan = (DropDatabasePlan) plan;
        name = dropDatabasePlan.getDatabaseName();
        // 判断是否已经存在这个database，如果不存在则返回fail，否则删除并返回success
        if (Manager.getInstance().containDatabase(name)) {
          Manager.getInstance().deleteDatabase(name);
          return new ExecuteStatementResp(
              StatusUtil.success("Database " + name + " dropped."), false);
        } else {
          return new ExecuteStatementResp(StatusUtil.fail("Database does not exist."), false);
        }
        // return new ExecuteStatementResp(StatusUtil.fail("Drop database is not supported."),
        // false);
      case USE_DB:
        System.out.println("IServiceHandler: [DEBUG] " + plan);
        UseDatabasePlan useDatabasePlan = (UseDatabasePlan) plan;
        name = useDatabasePlan.getDatabaseName();
        // 判断是否已经存在这个database，如果不存在则返回fail，否则设置manager中的currentDB后返回success
        if (Manager.getInstance().containDatabase(name)) {
          Manager.getInstance().setCurrentDB(name);
          return new ExecuteStatementResp(
              StatusUtil.success("Switched to database " + name + " ."), false);
        } else {
          return new ExecuteStatementResp(StatusUtil.fail("Database does not exist."), false);
        }
      case CREATE_TABLE:
        // 在create当中就完成了table是否已经存在的判断
        try {
          System.out.println("IServiceHandler: [DEBUG] " + plan);
          CreateTablePlan createTablePlan = (CreateTablePlan) plan;
          name = createTablePlan.getTableName();
          Column[] columns = createTablePlan.getColumns();
          Manager.getInstance().getCurrentDB().create(name.toLowerCase(), columns);
          return new ExecuteStatementResp(StatusUtil.success("Table " + name + " created."), false);
        } catch (Exception e) {
          return new ExecuteStatementResp(StatusUtil.fail(e.getMessage()), false);
        }

      case SHOW_TABLE:
        System.out.println("IServiceHandler: [DEBUG] " + plan);
        ShowTablePlan showTablePlan = (ShowTablePlan) plan;
        name = showTablePlan.getTableName();
        String ret = Manager.getInstance().getCurrentDB().show(name.toLowerCase());
        return new ExecuteStatementResp(StatusUtil.success(ret), false);

      case SHOW_DATABASES:
        System.out.println("IServiceHandler: [DEBUG] " + plan);
        ShowDatabasesPlan showDatabasesPlan = (ShowDatabasesPlan) plan;
        String ret1 = Manager.getInstance().showDatabases();
        return new ExecuteStatementResp(StatusUtil.success(ret1), false);
      case SHOW_DATABASE_META:
        System.out.println("IServiceHandler: [DEBUG] " + plan);
        ShowDatabaseMetaPlan showDatabaseMetaPlan = (ShowDatabaseMetaPlan) plan;
        name = showDatabaseMetaPlan.getDatabaseName();
        String ret2 = Manager.getInstance().showDatabaseMeta(name.toLowerCase());
        return new ExecuteStatementResp(StatusUtil.success(ret2), false);
      case DROP_TABLE:
        System.out.println("IServiceHandler: [DEBUG] " + plan);
        DropTablePlan dropTablePlan = (DropTablePlan) plan;
        name = dropTablePlan.getTableName();
        Manager.getInstance().getCurrentDB().drop(name.toLowerCase());
        return new ExecuteStatementResp(StatusUtil.success("Table " + name + " dropped."), false);

      case SELECT:
        try {
          System.out.println("IServiceHandler: [DEBUG] " + plan);
          SelectPlan selectPlan = (SelectPlan) plan;
          String[] columnsName = selectPlan.getColumns();
          ArrayList<String> tableNames = selectPlan.getTableNames();
          QueryTable queryTable = null; // = selectPlan.getQueryTable();
          int joinType = selectPlan.getJoinType();
          Logic logic = selectPlan.getLogic();
          Boolean distinct = selectPlan.getDistinct();
          // 如果没有join，即为单一表查询
          if (joinType == 0) {
            if (tableNames.size() != 1) {
              if (autoCommit) LogicalGenerator.generate("autocommit", sessionId);
              return new ExecuteStatementResp(StatusUtil.fail("wrong table size"), false);
            }
            queryTable =
                Manager.getInstance()
                    .getCurrentDB()
                    .BuildSingleQueryTable(tableNames.get(0), selectPlan.getWhereLogic());
          }
          // 如果是复合表，需要读取join逻辑和类型  0: no join，1: left join，2: right join 3: full join 4: 正常inner
          // join
          else {
            queryTable =
                Manager.getInstance()
                    .getCurrentDB()
                    .BuildJointQueryTable(tableNames, logic, joinType, selectPlan.getWhereLogic());
          }

          if (Global.DATABASE_ISOLATION_LEVEL == Global.ISOLATION_LEVEL.READ_COMMITTED) {
            ArrayList<String> table_s_list = Manager.getInstance().s_lock_dict.get(sessionId);
            // 打印出此时的事务号，以及此时的s锁列表
            System.out.println("session id: " + sessionId);
            System.out.println("s lock list: " + table_s_list);
            System.out.println("x lock dict: " + Manager.getInstance().x_lock_dict.get(sessionId));
            for (String table_name : table_s_list) {
              Table the_table = Manager.getInstance().getCurrentDB().get(table_name);
              the_table.free_s_lock(sessionId);
              the_table.quit_tran();
            }
            table_s_list.clear();
            Manager.getInstance().s_lock_dict.put(sessionId, table_s_list);
          }
          ExecuteStatementResp resp =
              new ExecuteStatementResp(StatusUtil.success("select result:"), true);
          QueryResult result =
              Manager.getInstance().getCurrentDB().select(columnsName, queryTable, distinct);
          if (result == null || result.mResultList.size() == 0) {
            resp.columnsList = new ArrayList<>();
            resp.rowList = new ArrayList<>();
          }
          for (String column_name : result.mColumnName) {
            resp.addToColumnsList(column_name);
          }
          for (Row row : result.mResultList) {
            ArrayList<String> the_result = row.toStringList();
            resp.addToRowList(the_result);
          }
          if (autoCommit) LogicalGenerator.generate("autocommit", sessionId);
          return resp;
        } catch (Exception e) {
          // QueryResult error_result = new QueryResult(e.toString());
          // return error_result;
          if (autoCommit) LogicalGenerator.generate("autocommit", sessionId);
          return new ExecuteStatementResp(StatusUtil.fail(e.getMessage()), false);
        }
      case INSERT:
        System.out.println("IServiceHandler: [DEBUG] " + plan);
        InsertPlan insertPlan = (InsertPlan) plan;
        String table_name = insertPlan.getTableName();
        String[] column_names = insertPlan.getColumnNames();
        List<SQLParser.ValueEntryContext> value_entrys = insertPlan.getValueEntryContextList();
        for (SQLParser.ValueEntryContext value_entry : value_entrys) {
          int size = value_entry.literalValue().size();
          String[] values = new String[size];
          for (int i = 0; i < size; i++) {
            values[i] = value_entry.literalValue(i).getText();
          }
          try {
            Manager.getInstance().getCurrentDB().insert(table_name, column_names, values);
          } catch (RuntimeException e) {
            if (autoCommit) LogicalGenerator.generate("autocommit", sessionId);
            return new ExecuteStatementResp(StatusUtil.fail(e.getMessage()), false);
          }
        }
        System.out.println("insert over");
        if (autoCommit) LogicalGenerator.generate("autocommit", sessionId);
        return new ExecuteStatementResp(StatusUtil.success("Insert successfully."), false);
      case UPDATE:
        try {
          System.out.println("IServiceHandler: [DEBUG] " + plan);
          UpdatePlan updatePlan = (UpdatePlan) plan;
          String table_name_for_update = updatePlan.getTableName();
          String column_name_for_update = updatePlan.getColumnName();
          Comparer value_for_update = updatePlan.getValue();
          Logic logic_for_update = updatePlan.getLogic();
          Manager.getInstance()
              .getCurrentDB()
              .update(
                  table_name_for_update,
                  column_name_for_update,
                  value_for_update,
                  logic_for_update);
          if (autoCommit) LogicalGenerator.generate("autocommit", sessionId);
          return new ExecuteStatementResp(StatusUtil.success("Update successfully."), false);
        } catch (Exception e) {
          System.out.println(e.getMessage());
          if (autoCommit) LogicalGenerator.generate("autocommit", sessionId);
          return new ExecuteStatementResp(StatusUtil.fail(e.getMessage()), false);
        }
      case DELETE:
        System.out.println("IServiceHandler: [DEBUG] " + plan);
        DeletePlan deletePlan = (DeletePlan) plan;
        String delete_table_name = deletePlan.getTableName();
        Logic delete_logic = deletePlan.getCondition();
        try {
          Manager.getInstance().getCurrentDB().delete(delete_table_name, delete_logic);
        } catch (Exception e) {
          System.out.println(e.getMessage());
        }
        if (autoCommit) LogicalGenerator.generate("autocommit", sessionId);
        return new ExecuteStatementResp(StatusUtil.success("delete successfully."), false);
      case BEGIN_TRANSACTION:
        System.out.println("IServiceHandler: [DEBUG] " + plan);
        return new ExecuteStatementResp(
            StatusUtil.success("Begin transaction successfully."), false);
      case COMMIT:
        System.out.println("IServiceHandler: [DEBUG] " + plan);
        return new ExecuteStatementResp(StatusUtil.success("Commit successfully."), false);
      case AUTO_BEGIN_TRANSACTION:
        System.out.println("IServiceHandler: [DEBUG] " + plan);
        return new ExecuteStatementResp(
            StatusUtil.success("Auto begin transaction successfully."), false);
      case AUTO_COMMIT:
        System.out.println("IServiceHandler: [DEBUG] " + plan);
        return new ExecuteStatementResp(StatusUtil.success("Auto commit successfully."), false);
      case ALTER_TABLE:
        System.out.println("IServiceHandler: [DEBUG] " + plan);
        AlterTablePlan alterTablePlan = (AlterTablePlan) plan;
        String alter_table_name = alterTablePlan.getTableName();
        String alter_column_name = alterTablePlan.getColumnName();
        String alter_operation_type = alterTablePlan.getOpType();
        ColumnType alter_column_type = null;
        int max_length = -1;
        // 准备相关参数
        if (alter_operation_type.equals("add")) {
          alter_column_type = alterTablePlan.getColumnType();
          max_length = alterTablePlan.getMaxLength();
        } else if (alter_operation_type.equals("drop")) {
          // do nothing
        } else {
          return new ExecuteStatementResp(StatusUtil.fail("wrong alter operation type"), false);
        }

        try {
          Manager.getInstance()
              .getCurrentDB()
              .alter(
                  alter_table_name,
                  alter_operation_type,
                  alter_column_name,
                  alter_column_type,
                  max_length);
        } catch (Exception e) {
          System.out.println(e.getMessage());
          return new ExecuteStatementResp(StatusUtil.fail(e.getMessage()), false);
        }
        return new ExecuteStatementResp(StatusUtil.success("Alter table successfully."), false);

      default:
    }
    return null;
  }
}
