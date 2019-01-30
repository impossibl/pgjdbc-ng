
public class Address implements SQLData {
  private static final String TYPE_NAME = "public.address";

  private String street;

  private String city;

  private String state;

  private String zip;

  @Override
  public String getSQLTypeName() throws SQLException {
    return TYPE_NAME;
  }

  public String getStreet() {
    return street;
  }

  public void setStreet(String street) {
    this.street = street;
  }

  public String getCity() {
    return city;
  }

  public void setCity(String city) {
    this.city = city;
  }

  public String getState() {
    return state;
  }

  public void setState(String state) {
    this.state = state;
  }

  public String getZip() {
    return zip;
  }

  public void setZip(String zip) {
    this.zip = zip;
  }

  @Override
  public void readSQL(SQLInput in, String typeName) throws SQLException {
    this.street = in.readString();
    this.city = in.readString();
    this.state = in.readString();
    this.zip = in.readString();
  }

  @Override
  public void writeSQL(SQLOutput out) throws SQLException {
    out.writeString(this.street);
    out.writeString(this.city);
    out.writeString(this.state);
    out.writeString(this.zip);
  }
}

public enum Title {
  MR("mr"),

  MRS("mrs"),

  MS("ms"),

  DR("dr");

  private String label;

  Title(String label) {
    this.label = label;
  }

  public String getLabel() {
    return label;
  }

  public static Title valueOfLabel(String label) {
    for (Title value : values()) {
      if (value.label.equals(label)) return value;
    }
    throw new IllegalArgumentException("Invalid label");}
}


public class VCard implements SQLData {
  private static final String TYPE_NAME = "public.v_card";

  private Integer id;

  private String name;

  private Title title;

  private PGStruct[] addresses;

  @Override
  public String getSQLTypeName() throws SQLException {
    return TYPE_NAME;
  }

  public Integer getId() {
    return id;
  }

  public void setId(Integer id) {
    this.id = id;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public Title getTitle() {
    return title;
  }

  public void setTitle(Title title) {
    this.title = title;
  }

  public PGStruct[] getAddresses() {
    return addresses;
  }

  public void setAddresses(PGStruct[] addresses) {
    this.addresses = addresses;
  }

  @Override
  public void readSQL(SQLInput in, String typeName) throws SQLException {
    this.id = in.readInt();
    this.name = in.readString();
    this.title = Title.valueOfLabel(in.readString());
    this.addresses = in.readObject(PGStruct[].class);
  }

  @Override
  public void writeSQL(SQLOutput out) throws SQLException {
    out.writeInt(this.id);
    out.writeString(this.name);
    out.writeString(this.title.getLabel());
    out.writeObject(this.addresses, null);
  }
}
