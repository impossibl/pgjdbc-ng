System.setOut(new FileOutputStream("table.txt")); // <1>
try (Statement statement = connection.createStatement()) {
  statement.execute("COPY a_table TO STDOUT"); // <2>
}
