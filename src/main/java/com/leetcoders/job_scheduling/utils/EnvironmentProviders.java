package com.leetcoders.job_scheduling.utils;

import io.github.cdimascio.dotenv.Dotenv;

public class EnvironmentProviders {
    public static String getPGServerIP() {
        Dotenv dotenv = Dotenv.configure().ignoreIfMissing().load();
        return dotenv.get("PG_SERVER_IP");
    }
    public static Integer getPGServerPort() {
        Dotenv dotenv = Dotenv.configure().ignoreIfMissing().load();
        return Integer.valueOf(dotenv.get("PG_SERVER_PORT"));
    }
    public static String getPGUsername() {
        Dotenv dotenv = Dotenv.configure().ignoreIfMissing().load();
        return dotenv.get("PG_SERVER_USERNAME");
    }

    public static String getPGPassword() {
        Dotenv dotenv = Dotenv.configure().ignoreIfMissing().load();
        return dotenv.get("PG_SERVER_PASSWORD");
    }

    public static String getKafkaServers() {
        Dotenv dotenv = Dotenv.configure().ignoreIfMissing().load();
        return dotenv.get("KAFKA_SERVERS");
    }
}
