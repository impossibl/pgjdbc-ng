System.setIn(new ByteArrayInputStream("1-1\t1-2\n2-1\t2-2\n3-1\t3-2".getBytes(UTF_8))); // <1>
try (Statement statement = connection.createStatement()) {
  statement.execute("COPY a_table FROM STDIN"); // <2>
}
