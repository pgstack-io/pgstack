import java.sql.*;

class CursorRead {
  public static void main(String[] args) throws Exception {
    try (Connection connection = DriverManager.getConnection(args[0])) {
      connection.setReadOnly(true);
      connection.setAutoCommit(false);
      // Use a fetch size that spans multiple internal result batches.
      try (PreparedStatement statement = connection.prepareStatement(
          "SELECT i, repeat(md5(i::text), 256) AS payload FROM generate_series(0, 11999) t(i) ORDER BY i")) {
        statement.setFetchSize(3677);
        if (statement.getMetaData().getColumnCount() != 2) throw new AssertionError("Statement Describe failed");
        try (ResultSet rows = statement.executeQuery()) {
          int count = 0;
          while (rows.next()) {
            if (rows.getLong(1) != count || rows.getString(2).length() != 8192) {
              throw new AssertionError("Unexpected row " + count);
            }
            count++;
          }
          if (count != 12000) throw new AssertionError("Read " + count + " rows");
        }
      }
      connection.commit();
      // Parameterized sample queries must still bind and complete normally.
      try (PreparedStatement statement = connection.prepareStatement("SELECT i FROM generate_series(0, 11999) t(i) LIMIT ?")) {
        statement.setLong(1, 1024);
        if (statement.getParameterMetaData().getParameterCount() != 1) throw new AssertionError("Parameter Describe failed");
        try (ResultSet rows = statement.executeQuery()) {
          int count = 0;
          while (rows.next()) count++;
          if (count != 1024) throw new AssertionError("Sample read " + count + " rows");
        }
      }
      connection.rollback();
      // Force a server-prepared statement and execute repeatedly so JDBC uses
      // binary input parameters after preparation. Use text results to keep this
      // focused on input decoding (binary result formats are a separate feature).
      try (PreparedStatement statement = connection.prepareStatement(
          "SELECT (?::real)::text, (?::double precision)::text, (?::boolean)::text")) {
        statement.unwrap(org.postgresql.PGStatement.class).setPrepareThreshold(1);
        for (int iteration = 0; iteration < 3; iteration++) {
          statement.setFloat(1, 1.5f);
          statement.setDouble(2, -123.125);
          statement.setBoolean(3, true);
          try (ResultSet rows = statement.executeQuery()) {
            if (!rows.next() || !rows.getString(1).equals("1.5")
                || !rows.getString(2).equals("-123.125") || !rows.getString(3).equals("true")
                || rows.next()) throw new AssertionError("Binary bind round trip failed");
          }
        }
      }
      connection.rollback();
      System.out.println("JDBC cursor: 12000 rows over multiple fetches; LIMIT sample: 1024 rows");
    }
  }
}
