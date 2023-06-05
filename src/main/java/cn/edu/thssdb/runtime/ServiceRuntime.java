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
import cn.edu.thssdb.sql.SQLParser;
import cn.edu.thssdb.utils.StatusUtil;

import java.util.ArrayList;
import java.util.List;

public class ServiceRuntime {

  public static ExecuteStatementResp executeStatement(String statement, long sessionId) {
    LogicalPlan plan = LogicalGenerator.generate(statement,sessionId); // 这里会调用parser解析语句
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
        // TODO 检查table是否已经存在～
        System.out.println("IServiceHandler: [DEBUG] " + plan);
        CreateTablePlan createTablePlan = (CreateTablePlan) plan;
        name = createTablePlan.getTableName();
        Column[] columns = createTablePlan.getColumns();
        Manager.getInstance().getCurrentDB().create(name.toLowerCase(), columns);
        return new ExecuteStatementResp(StatusUtil.success("Table " + name + " created."), false);

      case SHOW_TABLE:
        System.out.println("IServiceHandler: [DEBUG] " + plan);
        ShowTablePlan showTablePlan = (ShowTablePlan) plan;
        name = showTablePlan.getTableName();
        String ret = Manager.getInstance().getCurrentDB().show(name.toLowerCase());
        return new ExecuteStatementResp(StatusUtil.success(ret), false);

      case DROP_TABLE:
        System.out.println("IServiceHandler: [DEBUG] " + plan);
        DropTablePlan dropTablePlan = (DropTablePlan) plan;
        name = dropTablePlan.getTableName();
        Manager.getInstance().getCurrentDB().drop(name.toLowerCase());
        return new ExecuteStatementResp(StatusUtil.success("Table " + name + " dropped."), false);

      case SELECT:
        System.out.println("IServiceHandler: [DEBUG] " + plan);
        SelectPlan selectPlan = (SelectPlan) plan;
        String[] columnsName = selectPlan.getColumns();
        ArrayList<String> tableNames = selectPlan.getTableNames();
        QueryTable queryTable = null; // = selectPlan.getQueryTable();
        Logic logicForJoin = selectPlan.getLogicForJoin();
        Logic logic = selectPlan.getLogic();
        Boolean distinct = selectPlan.getDistinct();
        // 如果没有join，即为单一表查询
        if (logicForJoin == null) {
          if (tableNames.size() != 1) {
            return new ExecuteStatementResp(StatusUtil.fail("wrong table size"), false);
          }
          queryTable =
              Manager.getInstance().getCurrentDB().BuildSingleQueryTable(tableNames.get(0));
        }
        // 如果是复合表，需要读取join逻辑
        else {
          queryTable =
              Manager.getInstance().getCurrentDB().BuildJointQueryTable(tableNames, logicForJoin);
        }

        // TODO 后面要进行字符串处理哈哈哈哈这个太随便了
        String res = "";
        try {
          QueryResult result =
              Manager.getInstance().getCurrentDB().select(columnsName, queryTable, logic, distinct);
          for (String column_name : result.mColumnName) {
            res += column_name.toString() + ", ";
          }
          res += "\n------------------\n";
          for (Row row : result.mResultList) {
            ArrayList<String> the_result = row.toStringList();
            String tmp = the_result.toString();
            tmp = tmp.substring(1, tmp.length() - 1);
            res += tmp + "\n";
          }
          return new ExecuteStatementResp(StatusUtil.success(res), false);
        } catch (Exception e) {
          //QueryResult error_result = new QueryResult(e.toString());
          // return error_result;
          return new ExecuteStatementResp(StatusUtil.fail(e.getMessage()), false);
        }
      case INSERT:
        System.out.println("IServiceHandler: [DEBUG] " + plan);
        InsertPlan insertPlan = (InsertPlan) plan;
        // TODO read commit锁 目前先不加
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
            System.out.println("entered catch");
            return new ExecuteStatementResp(StatusUtil.fail(e.getMessage()), false);
            //System.out.println(e.getMessage());
            //return new ExecuteStatementResp(StatusUtil.fail(e.getMessage()), false);
          }
        }
        return new ExecuteStatementResp(StatusUtil.success("Insert successfully."), false);
      case UPDATE:
        System.out.println("IServiceHandler: [DEBUG] " + plan);
        UpdatePlan updatePlan = (UpdatePlan) plan;
        String table_name_for_update = updatePlan.getTableName();
        String column_name_for_update = updatePlan.getColumnName();
        Comparer value_for_update = updatePlan.getValue();
        Logic logic_for_update = updatePlan.getLogic();
        try {
          Manager.getInstance()
              .getCurrentDB()
              .update(
                  table_name_for_update,
                  column_name_for_update,
                  value_for_update,
                  logic_for_update);
        } catch (Exception e) {
          System.out.println(e.getMessage());
        }
        return new ExecuteStatementResp(StatusUtil.success("Update successfully."), false);
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
        return new ExecuteStatementResp(StatusUtil.success("delete successfully."), false);
//      case :


      default:
    }
    return null;
  }
}
