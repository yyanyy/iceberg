/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iceberg;

import java.util.List;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.iceberg.TestHelpers.assertSameSchemaMap;
import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;

@RunWith(Parameterized.class)
public class TestSchemaID extends TableTestBase {

  @Parameterized.Parameters(name = "formatVersion = {0}")
  public static Object[] parameters() {
    return new Object[] { 1, 2 };
  }

  public TestSchemaID(int formatVersion) {
    super(formatVersion);
  }

  @Test
  public void testNoChange() {
    // add files to table
    table.newAppend().appendFile(FILE_A).appendFile(FILE_B).commit();

    validateSingleSchema();
    validateSnapshotsAndHistoryEntries(1);

    // remove file from table
    table.newDelete().deleteFile(FILE_A).commit();

    validateSingleSchema();
    validateSnapshotsAndHistoryEntries(2);

    // add file to table
    table.newFastAppend().appendFile(FILE_A2).commit();

    validateSingleSchema();
    validateSnapshotsAndHistoryEntries(3);
  }

  @Test
  public void testSchemaIdChangeInSchemaUpdate() {
    // add files to table
    table.newAppend().appendFile(FILE_A).appendFile(FILE_B).commit();

    validateSingleSchema();
    validateSnapshotsAndHistoryEntries(1);

    // cache old schema
    Schema oldSchema = table.schema();

    // update schema
    table.updateSchema().addColumn("data2", Types.StringType.get()).commit();

    Schema updatedSchema = new Schema(1,
        required(1, "id", Types.IntegerType.get()),
        required(2, "data", Types.StringType.get()),
        optional(3, "data2", Types.StringType.get())
    );

    validateTwoSchemas(updatedSchema, oldSchema);
    validateSnapshotsAndHistoryEntries(1);

    // remove file from table
    table.newDelete().deleteFile(FILE_A).commit();

    validateTwoSchemas(updatedSchema, oldSchema);

    List<Snapshot> snapshots = Lists.newArrayList(table.snapshots());
    Assert.assertEquals("Number of snapshot should match",
        2, snapshots.size());
    Assert.assertEquals("First schema id within snapshots should match",
        Integer.valueOf(0), snapshots.get(0).schemaId());
    Assert.assertEquals("Second schema id within snapshots should match",
        Integer.valueOf(1), snapshots.get(1).schemaId());

    Assert.assertEquals("Number of history entries should match",
        2, table.history().size());
    Assert.assertEquals("First history entry id within snapshots should match",
        Integer.valueOf(0), table.history().get(0).schemaId());
    Assert.assertEquals("Second history entry id within snapshots should match",
        Integer.valueOf(1), table.history().get(1).schemaId());

    // add files to table
    table.newAppend().appendFile(FILE_A2).commit();

    validateTwoSchemas(updatedSchema, oldSchema);

    snapshots = Lists.newArrayList(table.snapshots());
    Assert.assertEquals("Number of snapshot should match",
        3, snapshots.size());
    Assert.assertEquals("First schema id within snapshots should match",
        Integer.valueOf(0), snapshots.get(0).schemaId());
    Assert.assertEquals("Second schema id within snapshots should match",
        Integer.valueOf(1), snapshots.get(1).schemaId());
    Assert.assertEquals("Third schema id within snapshots should match",
        Integer.valueOf(1), snapshots.get(2).schemaId());

    Assert.assertEquals("Number of history entries should match",
        3, table.history().size());
    Assert.assertEquals("First history entry id within snapshots should match",
        Integer.valueOf(0), table.history().get(0).schemaId());
    Assert.assertEquals("Second history entry id within snapshots should match",
        Integer.valueOf(1), table.history().get(1).schemaId());
    Assert.assertEquals("Third history entry id within snapshots should match",
        Integer.valueOf(1), table.history().get(2).schemaId());
  }

  private void validateSingleSchema() {
    Assert.assertEquals("Current schema ID should match",
        0, table.schema().schemaId());
    assertSameSchemaMap(ImmutableMap.of(0, table.schema()), table.schemas());
  }

  private void validateTwoSchemas(Schema updatedSchema, Schema oldSchema) {
    Assert.assertEquals("Current schema ID should match",
        1, table.schema().schemaId());
    Assert.assertEquals("Current schema should match",
        updatedSchema.asStruct(), table.schema().asStruct());
    assertSameSchemaMap(ImmutableMap.of(0, oldSchema, 1, updatedSchema), table.schemas());
  }

  private void validateSnapshotsAndHistoryEntries(int numElement) {
    List<Snapshot> snapshots = Lists.newArrayList(table.snapshots());
    Assert.assertEquals("Number of snapshot should match",
        numElement, snapshots.size());
    snapshots.forEach(snapshot ->
        Assert.assertEquals("Schema id within snapshots should match",
            Integer.valueOf(0), snapshot.schemaId()));

    Assert.assertEquals("Number of history entries should match",
        numElement, table.history().size());
    table.history().forEach(history ->
        Assert.assertEquals("Schema id within history entries should match",
            Integer.valueOf(0), history.schemaId()));
  }
}
