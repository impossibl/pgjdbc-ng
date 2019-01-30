import java.sql.SQLData;

class Address implements SQLData {

  public String street;
  public String city;
  public String state;
  public String zip;

  public String getSQLTypeName() { // <1>
    return "address";
  }

  public void readSQL(SQLInput stream, String typeName) throws java.sql.SQLException { // <2>
    street = stream.readString();
    city = stream.readString();
    state = stream.readString();
    zip = stream.readString();
  }

  public void writeSQL (SQLOutput stream) throws SQLException { // <3>
    stream.writeString(street);
    stream.writeString(city);
    stream.writeString(state);
    stream.writeString(zip);
  }

}
