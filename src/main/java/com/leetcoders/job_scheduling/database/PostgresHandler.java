package com.leetcoders.job_scheduling.database;

import com.leetcoders.job_scheduling.streams.UserRecommendationRefreshRequest;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

@Component
public class PostgresHandler {
    private static final String DB_NAME = "leetcode-rs";
    private HikariDataSource ds;
    private static Logger logger = LoggerFactory.getLogger(PostgresHandler.class);

    private static final String UPDATE_TIMED_OUT_CLIENTS = """
            UPDATE user_details set being_processed=False WHERE now() AT TIME ZONE 'UTC' > time_to_update;
            """;

    private static final String GET_TIMED_OUT_CLIENTS = """
            SELECT name FROM user_details WHERE now() AT TIME ZONE 'UTC' > time_to_update;""";
    private static final String MARK_CLIENT = """
            UPDATE user_details SET being_processed=True WHERE name=? AND being_processed=False;
            """;

    private static final String UNMARK_CLIENT = """
            UPDATE user_details SET being_processed=False, time_to_update=now() AT TIME ZONE 'UTC' + INTERVAL '5 HOURS'
            WHERE name=? AND being_processed=TRUE;
            """;
    private static final String GET_CLIENT_DETAILS = """
            SELECT name, access_key, companies from user_details WHERE name = ?;
            """;

    public PostgresHandler(String dbUrl, int dbPort, String dbUser, String dbPassword) {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(String.format("jdbc:postgresql://%s:%d/%s", dbUrl, dbPort, DB_NAME));
        config.setUsername(dbUser);
        config.setPassword(dbPassword);
        config.setMaximumPoolSize(10);
        config.addDataSourceProperty("cachePrepStmts", "true");
        config.addDataSourceProperty("prepStmtCacheSize", "250");
        config.addDataSourceProperty("prepStmtCacheSqlLimit", "2048");
        config.addDataSourceProperty("connectionTimeout", "30000");
        config.setAutoCommit(false);
        ds = new HikariDataSource(config);
        logger.info("Postgres handler initialized");
    }

    public List<String> getClientsPendingForExecution() {
        try (var connection = ds.getConnection(); var statement = connection.createStatement()) {
            statement.execute(UPDATE_TIMED_OUT_CLIENTS);
            connection.commit();
            var results = connection.createStatement().executeQuery(GET_TIMED_OUT_CLIENTS);
            List<String> res = new ArrayList<>();
            while (results.next()) {
                res.add(results.getString("name"));
            }
            return res;
        } catch (SQLException e) {
            logger.error(String.format("Failed to get pending clients for execution, due to %s", e.getMessage()));
            return List.of();
        }
    }

    public boolean tryToMarkClientAsBusy(String name) {
        try (var connection = ds.getConnection(); var statement = connection.prepareStatement(MARK_CLIENT)) {
            statement.setString(1, name);

            var result = statement.executeUpdate() > 0;
            connection.commit();
            return result;
        } catch (SQLException e) {
            logger.error(String.format("Failed to mark client as busy, due to %s", e.getMessage()));
            return false;
        }
    }

    public void releaseClient(String name) {
        try (var connection = ds.getConnection(); var statement = connection.prepareStatement(UNMARK_CLIENT)) {
            statement.setString(1, name);
            statement.executeUpdate();
            connection.commit();
        } catch (SQLException e) {
            logger.error(String.format("Failed to un-mark client as busy, due to %s", e.getMessage()));
        }
    }

    public UserRecommendationRefreshRequest getClientDetails(String name) {
        try (var connection = ds.getConnection(); var statement = connection.prepareStatement(GET_CLIENT_DETAILS)) {
            statement.setString(1, name);

            ResultSet results = statement.executeQuery();
            if (results.next()) {
                return new UserRecommendationRefreshRequest(results.getString(1),
                        results.getString(2),
                        Arrays.stream(((String[]) results.getArray(3).getArray())).toList());
            }
            return null;
        } catch (SQLException e) {
            logger.error(String.format("Failed to get client details, due to %s", e.getMessage()));
            return null;
        }
    }

}
