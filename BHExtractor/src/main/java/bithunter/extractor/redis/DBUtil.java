package bithunter.extractor.redis;

import com.mchange.v2.c3p0.ComboPooledDataSource;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.ResultSetHandler;

import javax.sql.DataSource;

import java.sql.ResultSet;
import java.sql.SQLException;

public class DBUtil {
	private static final DataSource DataSource;

	static {
		DataSource = new ComboPooledDataSource();
	}

	public static DataSource getDataSource() {
		return DataSource;
	}

	public static int[] batch(String sql, Object[][] params) throws SQLException {
		return new QueryRunner(getDataSource()).batch(sql, params);
	}

	public static int execute(String sql, Object... params) throws SQLException {
		return new QueryRunner(getDataSource()).execute(sql, params);
	}
}
