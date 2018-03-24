package com.alibaba.datax.plugin.reader.hivereader;

import com.alibaba.datax.common.element.*;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.common.spi.Reader;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.google.common.base.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;


/**
 * @auth: ronghua.yu
 * @time: 16/11/1
 * @desc:
 */
public class HiveReader extends Reader {
    public static class Job extends Reader.Job {
        private static final Logger LOG = LoggerFactory
                .getLogger(Job.class);

        private Configuration originalConfig = null;

        @Override
        public void init() {
            this.originalConfig = super.getPluginJobConf();

        }

        @Override
        public void preCheck(){
            init();
        }

        @Override
        public List<Configuration> split(int adviceNumber) {
            // hive暂时不支持字段切分
            List<Configuration> confs = new ArrayList<Configuration>();
            confs.add(this.originalConfig.clone());
            return confs;
        }

        @Override
        public void post() {
        }

        @Override
        public void destroy() {
        }

    }

    public static class Task extends Reader.Task {
        private static final Logger LOG = LoggerFactory
                .getLogger(Task.class);

        private static String driverName =
                "org.apache.hive.jdbc.HiveDriver";

        private Configuration readerSliceConfig;

        private String address = null;
        private String username = null;
        private String password = null;
        private String tablename = null;
        private HiveUtil hiveutil = null;

        private JSONArray hiveColumnMeta = null;
        private String where = null;

        private Connection conn;

        private Statement stmt;

        @Override
        public void init() {
            this.readerSliceConfig = super.getPluginJobConf();
            this.address = readerSliceConfig.getString(KeyConstant.HIVE_ADDRESS);
            this.tablename = readerSliceConfig.getString(KeyConstant.HIVE_TABLE);
            this.username = readerSliceConfig.getString(KeyConstant.HIVE_USERNAME);
            this.password = readerSliceConfig.getString(KeyConstant.HIVE_PASSWORD);

            this.hiveColumnMeta = JSON.parseArray(readerSliceConfig.getString(KeyConstant.COLUMN));
            this.where = readerSliceConfig.getString(KeyConstant.WHERE);

            if(Strings.isNullOrEmpty(address) || Strings.isNullOrEmpty(tablename)
                    || Strings.isNullOrEmpty(username) || Strings.isNullOrEmpty(password)) {
                throw DataXException.asDataXException(HiveReaderErrorCode.ILLEGAL_VALUE,
                        HiveReaderErrorCode.ILLEGAL_VALUE.getDescription());
            }

            try {
                hiveutil=new HiveUtil(this.readerSliceConfig);
                Class.forName(driverName);
            } catch (Throwable e) {
                throw DataXException.asDataXException(HiveReaderErrorCode.CLASS_EXCEPTION,
                        HiveReaderErrorCode.CLASS_EXCEPTION.getDescription());
            }
            try {
                conn = DriverManager.getConnection(
                        address, username, password);
                stmt = conn.createStatement();
            } catch (SQLException e) {
                throw DataXException.asDataXException(HiveReaderErrorCode.ILLEGAL_ADDRESS,
                        HiveReaderErrorCode.ILLEGAL_ADDRESS.getDescription());
            }
        }

        @Override
        public void startRead(RecordSender recordSender) {
            String sql = getSelectSql();
            try {
                ResultSet res = null;
                res = this.stmt.executeQuery(sql);
                // res.setFetchSize(1000);
                ResultSetMetaData metaData = res.getMetaData();
                while(res.next()) {
                    Record record = recordSender.createRecord();
                    for (int i = 1; i <= metaData.getColumnCount(); ++i) {
                        String type;
                        try {
                            type = metaData.getColumnTypeName(i);
                        } catch (SQLException e) {
                            // 因为sql的类型中没有NULL，所以需要自己捕获处理
                            if (e.getMessage().equals("Unrecognized column type: NULL")) {
                                type = "null";
                            } else {
                                throw e;
                            }
                        }

                        switch (ColumnType.getByTypeName(type)) {
                            case SMALLINT:
                            case TINYINT:
                            case INT:
                            case BIGINT:
                                record.addColumn(new LongColumn(res.getLong(i)));
                                break;
                            case BOOLEAN:
                                record.addColumn(new BoolColumn(res.getBoolean(i)));
                                break;
                            case FLOAT:
                            case DOUBLE:
                                record.addColumn(new DoubleColumn(res.getDouble(i)));
                                break;
                            case STRING:
                                record.addColumn(new StringColumn(res.getString(i)));
                                break;
                            case DATE:
                                record.addColumn(new DateColumn(res.getDate(i)));
                                break;
                            case NULL:
                                record.addColumn(new NullColumn());
                                break;
                            case ARRAY:
                            case OBJECT:
                                record.addColumn(new StringColumn(res.getString(i)));
                                break;
                            default:
                                record.addColumn(new StringColumn(res.getString(i)));
                                //throw DataException.asDataException(DBUtilErrorCode.TYPE_INVALID,
                                //        "hive字段类型不支持，请改变字段类型或联系开发人员! 字段类型：" + metaData.getColumnTypeName(i));
                        }
                    }
                    recordSender.sendToWriter(record);
                }
            } catch (SQLException e) {
                throw DataXException.asDataXException(HiveReaderErrorCode.EXECUTE_EXCEPTION,
                        HiveReaderErrorCode.EXECUTE_EXCEPTION.getDescription());
            }
        }

        @Override
        public void post() {
        }

        @Override
        public void destroy() {
        }

        private String getSelectSql() {
            StringBuffer buffer = new StringBuffer();
            buffer.append("select ");
            StringBuffer columnStr = new StringBuffer();

            Iterator columnItera = hiveColumnMeta.iterator();
            while (columnItera.hasNext()) {
                if (columnStr != null && columnStr.length() != 0) {
                    columnStr.append(", ");
                }

                JSONObject column = (JSONObject)columnItera.next();
                String name = column.getString(KeyConstant.NAME);
                String type = column.getString(KeyConstant.TYPE);

                if (name == null || type == null) {
                    continue;
                }
                name = name.toLowerCase();
                    columnStr.append(name);
            }

            buffer.append(columnStr);
            buffer.append(" from `" + tablename + "` ");
            if (where != null) {
                buffer.append("where " + where);
            }
            LOG.info("select sql: {}", buffer.toString());
            return buffer.toString();
        }
    }

}