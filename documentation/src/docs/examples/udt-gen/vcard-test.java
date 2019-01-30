
Address address = new Address();        // <1>
address.setStreet("123 Easy Street");
address.setCity("Easy Town");
address.setState("IL");
address.setZip("12345");

VCard vCard = new VCard();              // <2>
vCard.setId(5);
vCard.setTitle(Title.MR);
vCard.setName("A Guy");
vCard.setAddresses(new Address[]{address});

PreparedStatement stmt = connection.prepareStatement("INSERT INTO contacts(card) VALUES (?)");
stmt.setObject(1, vCard); // <3>

Statement stmt = connection.createStatement();
ResultSet rs = stmt.executeQuery("SELECT card FROM contacts;")
while (rs.next()) {
  VCard vCard = (VCard) rs.getObject(1, VCard.class); // <4>

  HashMap<String, Class<?>> customTypes = new HashMap<>(); // <5>
  customTypes.put("v_card", VCard.class);
  customTypes.put("address", Address.class);

  VCard vCard = (VCard) rs.getObject(1, customTypes); // <6>
}
