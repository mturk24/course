package edu.berkeley.cs186.database.table;

import edu.berkeley.cs186.database.StudentTest;
import edu.berkeley.cs186.database.TestUtils;
import edu.berkeley.cs186.database.databox.*;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;

public class TestSchema {



  
  @Test
  @Category(StudentTest.class)
  public void testToStringFieldsAndSizes() {
    Schema schema = TestUtils.createSchemaWithAllTypes();
    Record input = TestUtils.createRecordWithAllTypes();
    List<String> newList = new ArrayList<String>();
    for (DataBox d : input.getValues()) {
      if (d.type() == DataBox.Types.BOOL) {
        newList.add(Boolean.toString(d.getBool()));
      }
      if (d.type() == DataBox.Types.INT) {
        newList.add(Integer.toString(d.getInt()));
      }
      if (d.type() == DataBox.Types.STRING) {
        newList.add(d.getString());
      }
      if (d.type() == DataBox.Types.FLOAT) {
        newList.add(Float.toString(d.getFloat()));
      }
    }
    assertEquals(schema.getFieldNames().size(), newList.size());
  }


  @Test
  @Category(StudentTest.class)
  public void testRecordEquality() {
    Record input = TestUtils.createRecordWithAllTypes();
    Schema schema = TestUtils.createSchemaWithAllTypes();
    byte[] encoded = schema.encode(input);
    byte[] otherEncoded = schema.encode(input);
    Record decoded = schema.decode(encoded);
    Record otherDecoded = schema.decode(otherEncoded);
    assertEquals(decoded.equals(otherDecoded), otherDecoded.equals(decoded));
  }


  @Test
  @Category(StudentTest.class)
  public void testRecordTypeEntityCheck() {
    Record input = TestUtils.createRecordWithAllTypes();
    List<DataBox> dataValues = new ArrayList<DataBox>();
    dataValues.add(new BoolDataBox(true));
    dataValues.add(new IntDataBox(1));
    dataValues.add(new StringDataBox("abcde", 5));
    dataValues.add(new FloatDataBox((float) 1.2));
    int i = 0;
    for (DataBox d : input.getValues()) {
      assertEquals(d.type(), dataValues.get(i).type());
      i += 1;
    }
  }


  @Test
  public void testSchemaRetrieve() {
    Schema schema = TestUtils.createSchemaWithAllTypes();

    Record input = TestUtils.createRecordWithAllTypes();
    byte[] encoded = schema.encode(input);
    Record decoded = schema.decode(encoded);

    assertEquals(input, decoded);
  }

  @Test
  public void testValidRecord() {
    Schema schema = TestUtils.createSchemaWithAllTypes();
    Record input = TestUtils.createRecordWithAllTypes();

    try {
      Record output = schema.verify(input.getValues());
      assertEquals(input, output);
    } catch (SchemaException se) {
      fail();
    }
  }

  @Test(expected = SchemaException.class)
  public void testInvalidRecordLength() throws SchemaException {
    Schema schema = TestUtils.createSchemaWithAllTypes();
    schema.verify(new ArrayList<DataBox>());
  }

  @Test(expected = SchemaException.class)
  public void testInvalidFields() throws SchemaException {
    Schema schema = TestUtils.createSchemaWithAllTypes();
    List<DataBox> values = new ArrayList<DataBox>();

    values.add(new StringDataBox("abcde", 5));
    values.add(new IntDataBox(10));

    schema.verify(values);
  }

}
