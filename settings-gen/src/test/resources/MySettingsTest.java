package com.impossibl.postgres.system.test;

import com.impossibl.postgres.system.Setting;


@Setting.Factory
public class MySettingsTest {

  @Setting.Group.Info(
      id = "my", desc = "My Settings"
  )
  public static final Setting.Group MYGROUP = Setting.Group.declare();

  @Setting.Info(
      desc = "A String setting",
      name = "a.string",
      group = "my",
      def = "Hello Settings!",
      alternateNames = {"a", "string"}
  )
  public static final Setting<String> A = Setting.declare();

  @Setting.Info(
      desc = "An Integer setting",
      name = "b.int",
      group = "my",
      def = "10"
  )
  public static final Setting<Integer> B = Setting.declare();


  public enum AnEnum {

    @Setting.Description("This is a valid in UpperCamelCase")
    AValueInProperCase,

    @Setting.Description("A second value")
    AnotherValue
  }

  @Setting.Info(
      desc = "An Enum setting",
      name = "c.enum",
      group = "my",
      def = "a-value-in-proper-case"
  )
  public static final Setting<AnEnum> C = Setting.declare();


}
