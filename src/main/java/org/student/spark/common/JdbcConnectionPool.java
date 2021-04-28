package org.student.spark.common;

import com.typesafe.config.Config;
import com.zaxxer.hikari.HikariDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.ConcurrentHashMap;

import static java.util.concurrent.TimeUnit.SECONDS;

public class JdbcConnectionPool implements Serializable {

    final private HikariDataSource dataSource;
    final private static ConcurrentHashMap<String, JdbcConnectionPool> cacheMap = new ConcurrentHashMap<>();
    private static int counter = 0;

    final static Logger logger = LoggerFactory.getLogger(JdbcConnectionPool.class);

    private JdbcConnectionPool(Config config, String tag) {
        dataSource = new HikariDataSource();
        String url = config.getString("app.jdbc." + tag + ".url");
        String user = config.getString("app.jdbc." + tag + ".username");
        String password = config.getString("app.jdbc." + tag + ".password");
       // String driver = config.getString("app.jdbc." + tag + ".driver");
        dataSource.setJdbcUrl(url);
        dataSource.setUsername(user);
        dataSource.setPassword(password);
       // dataSource.setDriverClassName(driver);

        long connectionTimeout = CommonUtils.getLongWithDefault(config, Constants.JDBC_CONNECTION_TIMEOUT, 120);
        dataSource.setConnectionTimeout(SECONDS.toMillis(connectionTimeout));

        //dataSource.setMaximumPoolSize(20);
        counter++;
        logger.info("******** now we have jdbc connection pool size is " + counter);
    }

    public static JdbcConnectionPool getPool(Config config, String tag) {
        if (cacheMap.containsKey(tag)) {
            return cacheMap.get(tag);
        } else {
            synchronized (JdbcConnectionPool.class) {
                if (!cacheMap.containsKey(tag)) {
                    JdbcConnectionPool pool = new JdbcConnectionPool(config, tag);
                    cacheMap.putIfAbsent(tag, pool);
                }
            }
        }
        return cacheMap.get(tag);
    }

    public Connection getConnection(String tag) throws SQLException {
        return cacheMap.get(tag).dataSource.getConnection();
    }

}
