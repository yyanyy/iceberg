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
import java.util.stream.IntStream;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
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
    validateSnapshotsAndHistoryEntries(ImmutableList.of(0));

    // remove file from table
    table.newDelete().deleteFile(FILE_A).commit();

    validateSingleSchema();
    validateSnapshotsAndHistoryEntries(ImmutableList.of(0, 0));

    // add file to table
    table.newFastAppend().appendFile(FILE_A2).commit();

    validateSingleSchema();
    validateSnapshotsAndHistoryEntries(ImmutableList.of(0, 0, 0));
  }

  @Test
  public void testSchemaIdChangeInSchemaUpdate() {
    // add files to table
    table.newAppend().appendFile(FILE_A).appendFile(FILE_B).commit();

    validateSingleSchema();
    validateSnapshotsAndHistoryEntries(ImmutableList.of(0));

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
    validateSnapshotsAndHistoryEntries(ImmutableList.of(0));

    // remove file from table
    table.newDelete().deleteFile(FILE_A).commit();

    validateTwoSchemas(updatedSchema, oldSchema);
    validateSnapshotsAndHistoryEntries(ImmutableList.of(0, 1));

    // add files to table
    table.newAppend().appendFile(FILE_A2).commit();

    validateTwoSchemas(updatedSchema, oldSchema);
    validateSnapshotsAndHistoryEntries(ImmutableList.of(0, 1, 1));
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

  private void validateSnapshotsAndHistoryEntries(List<Integer> schemaIds) {
    List<Snapshot> snapshots = Lists.newArrayList(table.snapshots());
    Assert.assertEquals("Number of snapshot should match",
        schemaIds.size(), snapshots.size());
    IntStream.range(0, snapshots.size())
        .forEach(i -> Assert.assertEquals("Schema id within snapshots should match",
            schemaIds.get(i), snapshots.get(i).schemaId()));

    Assert.assertEquals("Number of history entries should match",
        schemaIds.size(), table.history().size());
    IntStream.range(0, table.history().size())
        .forEach(i -> Assert.assertEquals("Schema id within history entries should match",
            schemaIds.get(i), table.history().get(i).schemaId()));
  }
}
