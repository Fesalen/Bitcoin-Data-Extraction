package bithunter.extractor.redis;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Callable;

public class WakeUpConn implements Runnable {
    private Set<Connection> connections = Collections.synchronizedSet(new HashSet<>());

    public void register(Connection connection) {
        connections.add(connection);
    }

    @Override
    public void run() {
        for (Connection conn : connections) {
            try {
                Statement stat = conn.createStatement();
                stat.execute("Select 1");
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }
}
