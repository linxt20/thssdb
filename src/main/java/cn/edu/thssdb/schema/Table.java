package cn.edu.thssdb.schema;

import cn.edu.thssdb.exception.*;
import cn.edu.thssdb.storage.Cache;
import cn.edu.thssdb.type.ColumnType;
import cn.edu.thssdb.utils.Pair;

import java.io.File;
import java.io.FileInputStream;
import java.io.ObjectInputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static cn.edu.thssdb.utils.Global.storage_dir;

public class Table implements Iterable<Row> {
    ReentrantReadWriteLock lock;
    private String databaseName;
    public String tableName;
    public ArrayList<Column> columns;
    private int primaryIndex;
    public Cache cache;

    // TODO 暂时不考虑锁，后面再补充

    public Table(String databaseName, String tableName, Column[] columns) {
        System.out.println("==========Table constructor=============");
        this.lock = new ReentrantReadWriteLock();
        this.databaseName = databaseName;
        this.tableName = tableName;
        this.columns = new ArrayList<>();
        this.primaryIndex = -1;
        for (int i = 0; i < columns.length; i++) {
            this.columns.add(columns[i]);
            if (columns[i].getPrimary() == 1) {
                this.primaryIndex = i;
            }
        }
        // TODO primaryIndex如果没有被更新需要抛出异常
        this.cache = new Cache(databaseName, tableName);
        // TODO 一些后面要加的变量
        recover();
    }

    private void recover() {
        // TODO 需要修改
        File dir = new File(storage_dir);
        File[] fileList = dir.listFiles();
        if (fileList == null) return;

        HashMap<Integer, File> pageFileList = new HashMap<>();
        int pageNum = 0;
        for (File f : fileList) {
            if (f != null && f.isFile()) {
                try {

                    String[] parts = f.getName().split("\\.")[0].split("_");

                    String databaseName = parts[1];
                    String tableName = parts[2];

                    int id = Integer.parseInt(parts[3]);
                    if (!(this.databaseName.equals(databaseName) && this.tableName.equals(tableName)))
                        continue;
                    pageFileList.put(id, f);
                    if (id > pageNum) pageNum = id;
                } catch (Exception e) {
                    continue;
                }
            }
        }

        for (int i = 1; i <= pageNum; i++) {

            File f = pageFileList.get(i);
            ArrayList<Row> rows = deserialize(f);
            // TODO cache.insertPage(rows, primaryIndex);
        }
    }

    public String show() {
        String ret = "------------------------------------------------------\n";
        for (Column column : columns) {
            ret += column.show() + "\n";
        }
        ret += "------------------------------------------------------";
        return ret;
    }

    // TODO sgl
    public void insert(String[] columns, String[] values) {
        if (columns == null || values == null)
            throw new KeyNotExistException();
        // match columns and reorder entries
        int schemaLen = this.columns.size();
        if (columns.length > schemaLen) {
            throw new SchemaLengthMismatchException(schemaLen, columns.length);
        }
        else if(values.length > schemaLen) {
            throw new SchemaLengthMismatchException(schemaLen, values.length);
        }
        else if (columns.length != values.length) {
            throw new SchemaLengthMismatchException(columns.length, values.length);
        }
        ArrayList<Entry> orderedEntries = new ArrayList<>();
        for (Column column : this.columns)
        {
            int equal_num = 0;
            int place = -1;
            for (int i = 0; i < values.length; i++)
            {
                if (columns[i].equals(column.getName().toLowerCase()))
                {
                    place = i;
                    equal_num ++;
                }
            }
            if (equal_num > 1) {
                throw new DuplicateKeyException();
            }
            Comparable the_entry_value = null;
            if (equal_num == 0 || place < 0 || place >= columns.length)
            {
                the_entry_value = null;
            }
            else{
                the_entry_value = ParseValue(column, values[place]);
            }
            JudgeValid(column, the_entry_value);
            Entry the_entry = new Entry(the_entry_value);
            orderedEntries.add(the_entry);
        }

        // write to cache
        try {
            lock.writeLock().lock();
            cache.insertRow(orderedEntries, primaryIndex,false);
        }
        catch (DuplicateKeyException e) {
            throw e;
        }
        finally {
            lock.writeLock().unlock();
        }
    }

    private void JudgeValid(Column the_column, Comparable new_value) {
        boolean not_null = the_column.NotNull();
        ColumnType the_type = the_column.getType();
        int max_length = the_column.getMaxLength();
        if(not_null == true && new_value == null) {
            throw new NullValueException(the_column.getName());
        }
        if(the_type == ColumnType.STRING && new_value != null) {
            if(max_length >= 0 && (new_value + "").length() > max_length) {
                throw new TypeLengthMismatchException(the_column.getName(), max_length);
            }
        }
    }

    private Comparable ParseValue(Column the_column, String value) {
        if (value.equals("null")) {
            if (the_column.NotNull()) {
                throw new NullValueException(the_column.getName());
            }
            else {
                return null;
            }
        }
        switch (the_column.getType()) {
            case DOUBLE:
                return Double.parseDouble(value);
            case INT:
                return Integer.parseInt(value);
            case FLOAT:
                return Float.parseFloat(value);
            case LONG:
                return Long.parseLong(value);
            case STRING:
                return value.substring(1, value.length() - 1);
        }
        return null;
    }

    public void insert(String[] values) {
        if (values == null)
            throw new SchemaLengthMismatchException(this.columns.size(), 0);

        // match columns and reorder entries
        int schemaLen = this.columns.size();
        if(values.length > schemaLen) {
            throw new SchemaLengthMismatchException(schemaLen, values.length);
        }

        ArrayList<Entry> orderedEntries = new ArrayList<>();
        for (int i = 0; i < this.columns.size(); i ++)
        {
            Column column = this.columns.get(i);
            Comparable the_entry_value = null;
            if(i >= 0 && i < values.length) {
                the_entry_value = ParseValue(column, values[i]);
            }
            JudgeValid(column, the_entry_value);
            Entry the_entry = new Entry(the_entry_value);
            orderedEntries.add(the_entry);
        }

        // write to cache
        try {
            lock.writeLock().lock();
            cache.insertRow(orderedEntries, primaryIndex,false);
        }
        catch (DuplicateKeyException e) {
            throw e;
        }
        finally {
            lock.writeLock().unlock();
        }
    }

    public void insert() {
        // TODO
    }

    public void delete() {
        // TODO
    }

    public void update() {
        // TODO
    }

    private void serialize() {
        // TODO
    }

    public void dropSelf() {
        try {
            lock.writeLock().lock();
            // TODO cache
            // cache.dropSelf();
            // cache = null;

            File dir = new File(storage_dir);
            File[] fileList = dir.listFiles();
            if (fileList == null) return;
            for (File f : fileList) {
                if (f != null && f.isFile()) {
                    try {
                        String[] parts = f.getName().split("\\.")[0].split("_");
                        String databaseName = parts[1];
                        String tableName = parts[2];
                        if (!(this.databaseName.equals(databaseName) && this.tableName.equals(tableName)))
                            continue;
                    } catch (Exception e) {
                        continue;
                    }
                    f.delete();
                }
            }

            columns.clear();
            columns = null;
        } finally {
            lock.writeLock().unlock();
        }
    }

    private ArrayList<Row> deserialize(File file) {
        ArrayList<Row> rows;
        try {
            ObjectInputStream ois = new ObjectInputStream(new FileInputStream(file));
            rows = (ArrayList<Row>) ois.readObject();
            ois.close();
        } catch (Exception e) {
            rows = null;
        }
        return rows;
    }

    private class TableIterator implements Iterator<Row> {
        private Iterator<Pair<Entry, Row>> iterator;

        TableIterator(Table table) {
            this.iterator = table.cache.getIndexIter();
        }

        @Override
        public boolean hasNext() {
            return iterator.hasNext();
        }

        @Override
        public Row next() {
            return iterator.next().right;
        }
    }

    @Override
    public Iterator<Row> iterator() {
        return new TableIterator(this);
    }
}
