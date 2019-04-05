OutputStream fileOut = new FileOutputStream("table.txt")); // <1>
connection.unwrap(PGConnection.class).copyTo("COPY a_table TO STDOUT", fileOut); // <2>
