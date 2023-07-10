/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.tests.product.deltalake;

import io.trino.testng.services.Flaky;
import org.testng.annotations.Test;

import static io.trino.tempto.assertions.QueryAssert.Row.row;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.tests.product.TestGroups.DELTA_LAKE_DATABRICKS;
import static io.trino.tests.product.TestGroups.DELTA_LAKE_EXCLUDE_73;
import static io.trino.tests.product.TestGroups.DELTA_LAKE_OSS;
import static io.trino.tests.product.TestGroups.PROFILE_SPECIFIC_TESTS;
import static io.trino.tests.product.deltalake.util.DeltaLakeTestUtils.DATABRICKS_COMMUNICATION_FAILURE_ISSUE;
import static io.trino.tests.product.deltalake.util.DeltaLakeTestUtils.DATABRICKS_COMMUNICATION_FAILURE_MATCH;
import static io.trino.tests.product.deltalake.util.DeltaLakeTestUtils.dropDeltaTableWithRetry;
import static io.trino.tests.product.deltalake.util.DeltaLakeTestUtils.getColumnCommentOnDelta;
import static io.trino.tests.product.deltalake.util.DeltaLakeTestUtils.getColumnCommentOnTrino;
import static io.trino.tests.product.utils.QueryExecutors.onDelta;
import static io.trino.tests.product.utils.QueryExecutors.onTrino;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testng.Assert.assertEquals;

public class TestDeltaLakeCaseInsensitiveMapping
        extends BaseTestDeltaLakeS3Storage
{
    @Test(groups = {DELTA_LAKE_DATABRICKS, DELTA_LAKE_OSS, PROFILE_SPECIFIC_TESTS})
    @Flaky(issue = DATABRICKS_COMMUNICATION_FAILURE_ISSUE, match = DATABRICKS_COMMUNICATION_FAILURE_MATCH)
    public void testStatsWithNonLowerCaseColumnName()
    {
        String tableName = "test_dl_stats_uppercase_name" + randomNameSuffix();

        onDelta().executeQuery("" +
                "CREATE TABLE default." + tableName +
                "(UPPER_CASE INT, PART INT)" +
                "USING delta " +
                "PARTITIONED BY (PART)" +
                "LOCATION 's3://" + bucketName + "/databricks-compatibility-test-" + tableName + "'");

        try {
            onDelta().executeQuery("INSERT INTO default." + tableName + " VALUES (1, 10)");
            onTrino().executeQuery("INSERT INTO delta.default." + tableName + " VALUES (2, 20)");
            onTrino().executeQuery("INSERT INTO delta.default." + tableName + " VALUES (null, null)");

            assertThat(onTrino().executeQuery("SHOW STATS FOR delta.default." + tableName))
                    .containsOnly(
                            row("upper_case", null, 1.0, 0.33333333333, null, "1", "2"),
                            row("part", null, 2.0, 0.33333333333, null, null, null),
                            row(null, null, null, null, 3.0, null, null));
        }
        finally {
            dropDeltaTableWithRetry("default." + tableName);
        }
    }

    @Test(groups = {DELTA_LAKE_DATABRICKS, DELTA_LAKE_OSS, PROFILE_SPECIFIC_TESTS})
    @Flaky(issue = DATABRICKS_COMMUNICATION_FAILURE_ISSUE, match = DATABRICKS_COMMUNICATION_FAILURE_MATCH)
    public void testColumnCommentWithNonLowerCaseColumnName()
    {
        String tableName = "test_dl_column_comment_uppercase_name" + randomNameSuffix();

        onDelta().executeQuery("" +
                "CREATE TABLE default." + tableName +
                "(UPPER_CASE INT COMMENT 'test column comment')" +
                "USING delta " +
                "LOCATION 's3://" + bucketName + "/databricks-compatibility-test-" + tableName + "'");

        try {
            assertEquals(getColumnCommentOnTrino("default", tableName, "upper_case"), "test column comment");
            assertEquals(getColumnCommentOnDelta("default", tableName, "UPPER_CASE"), "test column comment");

            onTrino().executeQuery("COMMENT ON COLUMN delta.default." + tableName + ".upper_case IS 'test updated comment'");

            assertEquals(getColumnCommentOnTrino("default", tableName, "upper_case"), "test updated comment");
            assertEquals(getColumnCommentOnDelta("default", tableName, "UPPER_CASE"), "test updated comment");
        }
        finally {
            dropDeltaTableWithRetry("default." + tableName);
        }
    }

    @Test(groups = {DELTA_LAKE_DATABRICKS, DELTA_LAKE_OSS, PROFILE_SPECIFIC_TESTS})
    @Flaky(issue = DATABRICKS_COMMUNICATION_FAILURE_ISSUE, match = DATABRICKS_COMMUNICATION_FAILURE_MATCH)
    public void testNotNullColumnWithNonLowerCaseColumnName()
    {
        String tableName = "test_dl_notnull_column_uppercase_name" + randomNameSuffix();

        onDelta().executeQuery("" +
                "CREATE TABLE default." + tableName +
                "(UPPER_CASE INT NOT NULL)" +
                "USING delta " +
                "LOCATION 's3://" + bucketName + "/databricks-compatibility-test-" + tableName + "'");

        try {
            // Verify column operation doesn't delete NOT NULL constraint
            onTrino().executeQuery("COMMENT ON COLUMN delta.default." + tableName + ".upper_case IS 'test comment'");

            assertThatThrownBy(() -> onTrino().executeQuery("INSERT INTO delta.default." + tableName + " VALUES NULL"))
                    .hasMessageContaining("NULL value not allowed for NOT NULL column");
        }
        finally {
            dropDeltaTableWithRetry("default." + tableName);
        }
    }

    @Test(groups = {DELTA_LAKE_DATABRICKS, DELTA_LAKE_EXCLUDE_73, PROFILE_SPECIFIC_TESTS})
    @Flaky(issue = DATABRICKS_COMMUNICATION_FAILURE_ISSUE, match = DATABRICKS_COMMUNICATION_FAILURE_MATCH)
    public void testGeneratedColumnWithNonLowerCaseColumnName()
    {
        String tableName = "test_dl_generated_column_uppercase_name" + randomNameSuffix();

        onDelta().executeQuery("" +
                "CREATE TABLE default." + tableName +
                "(A INT, B INT GENERATED ALWAYS AS (A * 2))" +
                "USING delta " +
                "LOCATION 's3://" + bucketName + "/databricks-compatibility-test-" + tableName + "'");

        try {
            // Verify column operation doesn't delete generated expressions
            onTrino().executeQuery("COMMENT ON COLUMN delta.default." + tableName + ".a IS 'test comment for a'");
            onTrino().executeQuery("COMMENT ON COLUMN delta.default." + tableName + ".b IS 'test comment for b'");

            assertThatThrownBy(() -> onTrino().executeQuery("INSERT INTO delta.default." + tableName + " VALUES (1, 2)"))
                    .hasMessageContaining("Writing to tables with generated columns is not supported");
        }
        finally {
            dropDeltaTableWithRetry("default." + tableName);
        }
    }

    @Test(groups = {DELTA_LAKE_DATABRICKS, DELTA_LAKE_EXCLUDE_73, PROFILE_SPECIFIC_TESTS})
    @Flaky(issue = DATABRICKS_COMMUNICATION_FAILURE_ISSUE, match = DATABRICKS_COMMUNICATION_FAILURE_MATCH)
    public void testIdentityColumnWithNonLowerCaseColumnName()
    {
        String tableName = "test_dl_identity_column_uppercase_name" + randomNameSuffix();

        onDelta().executeQuery("" +
                "CREATE TABLE default." + tableName +
                "(UPPER_CASE BIGINT GENERATED ALWAYS AS IDENTITY)" +
                "USING delta " +
                "LOCATION 's3://" + bucketName + "/databricks-compatibility-test-" + tableName + "'");

        try {
            // Verify column operation doesn't delete NOT NULL constraint
            assertThatThrownBy(() -> onTrino().executeQuery("COMMENT ON COLUMN delta.default." + tableName + ".upper_case IS 'test comment'"))
                    .hasMessageContaining("Delta Lake writer version 6 which is not supported");

            assertThat((String) onDelta().executeQuery("SHOW CREATE TABLE default." + tableName).getOnlyValue())
                    .contains("UPPER_CASE BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1))");
        }
        finally {
            dropDeltaTableWithRetry("default." + tableName);
        }
    }
}
