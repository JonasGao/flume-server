package com.x.common.flume.sink.mysql;

import com.x.ecerp.model.common.ComApiLogModel;
import org.apache.commons.lang.time.FastDateFormat;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.namedparam.BeanPropertySqlParameterSource;
import org.springframework.jdbc.core.namedparam.SqlParameterSource;
import org.springframework.jdbc.core.simple.SimpleJdbcInsert;
import org.springframework.stereotype.Repository;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 * 插入 {@link ComApiLogModel}
 * Created by jonas on 2017/4/24.
 */
@Repository
public class ComApiLogDao {
    private static final Logger logger = LoggerFactory.getLogger(ComApiLogDao.class);

    private final static String TABLE_NAME = "com_api_log_%s";
    private final static FastDateFormat BASIC_DATE = FastDateFormat.getInstance("yyyyMM");

    private final JdbcTemplate jdbcTemplate;
    private final Map<String, Pair<String, SimpleJdbcInsert>> simpleJdbcInsertMap = new HashMap<>();

    @Autowired
    public ComApiLogDao(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public void insert(ComApiLogModel comApiLogModel) {
        String tableKey = getTableKey(comApiLogModel);
        String tableName = getTableName(comApiLogModel);
        SimpleJdbcInsert simpleJdbcInsert;

        Pair<String, SimpleJdbcInsert> pair = simpleJdbcInsertMap.get(tableKey);

        if (pair != null && !pair.getKey().equals(tableName)) {
            pair = null;
        }

        if (pair == null) {
            createTableLike(tableName);
            simpleJdbcInsert = new SimpleJdbcInsert(jdbcTemplate)
                    .withTableName(tableName)
                    .usingGeneratedKeyColumns("id");

            pair = Pair.of(tableName, simpleJdbcInsert);
            simpleJdbcInsertMap.put(tableKey, pair);
        } else {
            simpleJdbcInsert = pair.getValue();
        }

        logger.debug("插入语句模板 (注意第一次时为空) {}", simpleJdbcInsert.getInsertString());
        logger.debug("参数测试 {}, {}, {}, {}", comApiLogModel.getApimethodname(), comApiLogModel.getUsername(),
                comApiLogModel.getMac(), comApiLogModel.getThirdPart());

        SqlParameterSource parameters = new BeanPropertySqlParameterSource(comApiLogModel);
        simpleJdbcInsert.execute(parameters);

        logger.info("记录新数据 {}", comApiLogModel.getApimethodname());
    }

    String getTableName(ComApiLogModel comApiLogModel) {
        Date modified = comApiLogModel.getModified();
        String tableKey = getTableKey(comApiLogModel);
        String formatted;
        if (modified == null) {
            formatted = BASIC_DATE.format(new Date());
        } else {
            formatted = BASIC_DATE.format(modified);
        }
        return tableKey + "_" + formatted;
    }

    private String getTableKey(ComApiLogModel comApiLogModel) {
        return String.format(TABLE_NAME, comApiLogModel.getThirdPart());
    }

    private synchronized void createTableLike(String tableName) {
        if (!tableExists(tableName)) {
            jdbcTemplate.execute("CREATE TABLE " + tableName + " LIKE com_api_log");
        }
    }

    private boolean tableExists(String tableName) {
        return jdbcTemplate.queryForObject("SELECT COUNT(1) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = ? AND TABLE_SCHEMA = SCHEMA()",
                Integer.class, tableName) > 0;
    }
}
