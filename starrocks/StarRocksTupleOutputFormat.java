package starrocks;

import org.apache.flink.api.java.tuple.Tuple;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.List;

public class StarRocksTupleOutputFormat extends AbstractStarRocksOutputFormat<Tuple> {

    /**
     * StarRocksRowOutputFormat
     *
     * @param url
     * @param username
     * @param password
     * @param database
     * @param table
     * @param columns
     * @param batchInterval
     */
    public StarRocksTupleOutputFormat(
            String url,
            String username,
            String password,
            String database,
            String table,
            String[] columns,
            int batchInterval) {
        super(url, username, password, database, table, columns, batchInterval);
    }

    /**
     * 记录转化成字符串
     *
     * @param record
     * @return
     */
    @Override
    protected String convertRecordToString(Tuple record) {
        List<String> row = new ArrayList<>();
        int l = record.getArity();
        for (int i = 0; i < l; i++) {
            row.add(String.valueOf(record.getField(i)));
        }
        return String.join(columnSeperator, row);
    }

    public static StarRocksTupleOutputFormatBuilder buildOutputFormat() {
        return new StarRocksTupleOutputFormatBuilder();
    }

    public static class StarRocksTupleOutputFormatBuilder {
        /** 数据库链接url */
        private String url;
        /** 数据库用户名 */
        private String username;
        /** 数据库密码 */
        private String password;
        /** 数据库链接url */
        private String database;
        /** 数据库用户名 */
        private String table;
        /** 数据库密码 */
        private String[] columns;
        /** 批次大小 */
        private int batchInterval = DEFAULT_FLUSH_MAX_SIZE;

        protected StarRocksTupleOutputFormatBuilder() {}

        public StarRocksTupleOutputFormatBuilder setUrl(String url) {
            this.url = url;
            return this;
        }

        public StarRocksTupleOutputFormatBuilder setUsername(String username) {
            this.username = username;
            return this;
        }

        public StarRocksTupleOutputFormatBuilder setPassword(String password) {
            this.password = password;
            return this;
        }

        public StarRocksTupleOutputFormatBuilder setDatabase(String database) {
            this.database = database;
            return this;
        }

        public StarRocksTupleOutputFormatBuilder setTable(String table) {
            this.table = table;
            return this;
        }

        public StarRocksTupleOutputFormatBuilder setColumns(String[] columns) {
            this.columns = columns;
            return this;
        }

        public StarRocksTupleOutputFormatBuilder setBatchInterval(int batchInterval) {
            this.batchInterval = batchInterval;
            return this;
        }

        public StarRocksTupleOutputFormat finish() {
            if (StringUtils.isBlank(url)) {
                throw new IllegalArgumentException("No database URL supplied.");
            }

            if (StringUtils.isBlank(username)) {
                throw new IllegalArgumentException("No database username supplied.");
            }

            if (StringUtils.isBlank(table)) {
                throw new IllegalArgumentException("No database table supplied.");
            }

            if (ArrayUtils.isEmpty(columns)) {
                throw new IllegalArgumentException("No database columns supplied.");
            }

            return new StarRocksTupleOutputFormat(url, username, password, database, table, columns, batchInterval);
        }
    }
}
