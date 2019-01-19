package udt.test.vcard;

import udt.test.VCard;
import udt.test.Address;
import udt.test.Title;

class VCardTest {

  public static void test() {
    Address address = new Address();
    address.setStreet("123 Easy Street");
    address.setCity("Easy Town");
    address.setState("IL");
    address.setZip("12345");

    VCard vCard = new VCard();
    vCard.setId(5);
    vCard.setTitle(Title.MR);
    vCard.setName("A Guy");
    vCard.setAddresses(new Address[]{address});
  }

}
