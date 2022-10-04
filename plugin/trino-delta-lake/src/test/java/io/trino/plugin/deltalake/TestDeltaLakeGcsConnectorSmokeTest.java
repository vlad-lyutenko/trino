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
package io.trino.plugin.deltalake;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.ByteSource;
import com.google.common.io.ByteStreams;
import com.google.common.io.Resources;
import com.google.common.reflect.ClassPath;
import io.airlift.log.Logger;
import io.trino.hadoop.ConfigurationInstantiator;
import io.trino.plugin.hive.containers.HiveMinioDataLake;
import io.trino.plugin.hive.gcs.GoogleGcsConfigurationInitializer;
import io.trino.plugin.hive.gcs.HiveGcsConfig;
import io.trino.testing.QueryRunner;
import io.trino.testing.TestingConnectorBehavior;
import io.trino.testing.sql.TestTable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.RemoteIterator;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import java.io.ByteArrayInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.plugin.deltalake.DeltaLakeQueryRunner.DELTA_CATALOG;
import static io.trino.plugin.deltalake.DeltaLakeQueryRunner.createDockerizedDeltaLakeQueryRunner;
import static io.trino.plugin.hive.containers.HiveHadoop.HIVE3_IMAGE;
import static io.trino.testing.sql.TestTable.randomTableSuffix;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;
import static java.util.regex.Matcher.quoteReplacement;

/**
 * This test requires these variables to connect to GCS:
 * - gcp-storage-bucket: The name of the bucket to store tables in. The bucket must already exist.
 * - gcp-credentials-key: A base64 encoded copy of the JSON authentication file for the service account used to connect to GCP.
 *   For example, `cat service-account-key.json | base64`
 */
public class TestDeltaLakeGcsConnectorSmokeTest
        extends BaseDeltaLakeConnectorSmokeTest
{
    private static final Logger LOG = Logger.get(TestDeltaLakeGcsConnectorSmokeTest.class);
    private static final FileAttribute<?> READ_ONLY_PERMISSIONS = PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rw-r--r--"));

    private final String gcpStorageBucket;
    private final Path gcpCredentialsFile;
    private final FileSystem fileSystem;

    @Parameters({"testing.gcp-storage-bucket", "testing.gcp-credentials-key"})
    public TestDeltaLakeGcsConnectorSmokeTest(String gcpStorageBucket, String gcpCredentialKey)
    {
        this.gcpStorageBucket = requireNonNull(gcpStorageBucket, "gcpStorageBucket is null");

        requireNonNull(gcpCredentialKey, "gcpCredentialKey is null");
        InputStream jsonKey = new ByteArrayInputStream(Base64.getDecoder().decode(gcpCredentialKey));
        try {
            this.gcpCredentialsFile = Files.createTempFile("gcp-credentials", ".json", READ_ONLY_PERMISSIONS);
            gcpCredentialsFile.toFile().deleteOnExit();
            Files.write(gcpCredentialsFile, jsonKey.readAllBytes());

            HiveGcsConfig gcsConfig = new HiveGcsConfig().setJsonKeyFilePath(gcpCredentialsFile.toAbsolutePath().toString());
            Configuration configuration = ConfigurationInstantiator.newEmptyConfiguration();
            new GoogleGcsConfigurationInitializer(gcsConfig).initializeConfiguration(configuration);

            this.fileSystem = FileSystem.newInstance(new URI(bucketUrl()), configuration);
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }

    @AfterClass(alwaysRun = true)
    public void removeTestData()
    {
        if (fileSystem != null) {
            try {
                fileSystem.delete(new org.apache.hadoop.fs.Path(bucketUrl()), true);
            }
            catch (IOException e) {
                // The GCS bucket should be configured to expire objects automatically. Clean up issues do not need to fail the test.
                LOG.warn(e, "Failed to clean up GCS test directory: %s", bucketUrl());
            }
        }
    }

    @Override
    protected boolean hasBehavior(TestingConnectorBehavior connectorBehavior)
    {
        switch (connectorBehavior) {
            case SUPPORTS_INSERT:
            case SUPPORTS_DELETE:
            case SUPPORTS_UPDATE:
            case SUPPORTS_MERGE:
                return false;

            default:
                return super.hasBehavior(connectorBehavior);
        }
    }

    @Override
    protected HiveMinioDataLake createHiveMinioDataLake()
            throws Exception
    {
        String gcpSpecificCoreSiteXmlContent = Resources.toString(Resources.getResource("io/trino/plugin/deltalake/hdp3.1-core-site.xml.gcs-template"), UTF_8)
                .replace("%GCP_CREDENTIALS_FILE_PATH%", "/etc/hadoop/conf/gcp-credentials.json");

        Path hadoopCoreSiteXmlTempFile = Files.createTempFile("core-site", ".xml", READ_ONLY_PERMISSIONS);
        hadoopCoreSiteXmlTempFile.toFile().deleteOnExit();
        Files.writeString(hadoopCoreSiteXmlTempFile, gcpSpecificCoreSiteXmlContent);

        HiveMinioDataLake dataLake = new HiveMinioDataLake(
                bucketName,
                ImmutableMap.of(
                        "/etc/hadoop/conf/core-site.xml", hadoopCoreSiteXmlTempFile.normalize().toAbsolutePath().toString(),
                        "/etc/hadoop/conf/gcp-credentials.json", gcpCredentialsFile.toAbsolutePath().toString()),
                HIVE3_IMAGE);
        dataLake.start();
        return dataLake;
    }

    @Override
    protected QueryRunner createDeltaLakeQueryRunner(Map<String, String> connectorProperties)
            throws Exception
    {
        return createDockerizedDeltaLakeQueryRunner(
                DELTA_CATALOG,
                SCHEMA,
                ImmutableMap.of(),
                ImmutableMap.of(),
                ImmutableMap.<String, String>builder()
                        .putAll(connectorProperties)
                        .put("hive.gcs.json-key-file-path", gcpCredentialsFile.toAbsolutePath().toString())
                        .put("delta.unique-table-location", "false")
                        .buildOrThrow(),
                hiveMinioDataLake.getHiveHadoop(),
                queryRunner -> {});
    }

    @Override
    protected void createTableFromResources(String table, String resourcePath, QueryRunner queryRunner)
    {
        String targetDirectory = bucketName + "/" + table;

        try {
            List<ClassPath.ResourceInfo> resources = ClassPath.from(TestDeltaLakeAdlsConnectorSmokeTest.class.getClassLoader())
                    .getResources()
                    .stream()
                    .filter(resourceInfo -> resourceInfo.getResourceName().startsWith(resourcePath + "/"))
                    .collect(toImmutableList());
            for (ClassPath.ResourceInfo resourceInfo : resources) {
                String fileName = resourceInfo.getResourceName().replaceFirst("^" + Pattern.quote(resourcePath), quoteReplacement(targetDirectory));
                ByteSource byteSource = resourceInfo.asByteSource();
                try (FSDataOutputStream fileStream = fileSystem.create(new org.apache.hadoop.fs.Path(fileName), true)) {
                    ByteStreams.copy(byteSource.openBufferedStream(), fileStream);
                }
            }
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }

        queryRunner.execute(format("CREATE TABLE %s (dummy int) WITH (location = '%s')", table, getLocationForTable(bucketName, table)));
    }

    @Override
    protected String getLocationForTable(String bucketName, String tableName)
    {
        return bucketUrl() + tableName;
    }

    @Override
    protected List<String> getTableFiles(String tableName)
    {
        return listAllFilesRecursive(tableName);
    }

    @Override
    protected List<String> listCheckpointFiles(String transactionLogDirectory)
    {
        return listAllFilesRecursive(transactionLogDirectory).stream()
                .filter(path -> path.contains("checkpoint.parquet"))
                .collect(toImmutableList());
    }

    private List<String> listAllFilesRecursive(String directory)
    {
        String path = bucketUrl() + directory;
        ImmutableList.Builder<String> paths = ImmutableList.builder();

        try {
            RemoteIterator<LocatedFileStatus> files = fileSystem.listFiles(new org.apache.hadoop.fs.Path(path), true);
            while (files.hasNext()) {
                LocatedFileStatus file = files.next();
                if (!file.isDirectory()) {
                    paths.add(file.getPath().toString());
                }
            }
            return paths.build();
        }
        catch (FileNotFoundException e) {
            return ImmutableList.of();
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    protected String bucketUrl()
    {
        return format("gs://%s/%s/", gcpStorageBucket, bucketName);
    }

    // These overrides are required because the error message does not match the standard format
    @Override
    @Test
    public void testInsert()
    {
        assertQueryFails("INSERT INTO region (regionkey) VALUES (42)", "Inserts are not supported on the gs filesystem");
    }

    @Override
    @Test
    public void testUpdate()
    {
        assertQueryFails("UPDATE nation SET nationkey = nationkey + regionkey WHERE regionkey < 1", "Updates are not supported on the gs filesystem");
    }

    @Override
    @Test
    public void testMerge()
    {
        assertQueryFails("MERGE INTO nation n USING nation s ON (n.nationkey = s.nationkey) " +
                        "WHEN MATCHED AND n.regionkey < 1 THEN UPDATE SET nationkey = 5",
                "Updates are not supported on the gs filesystem");
    }

    @Override
    @Test
    public void verifySupportsDeleteDeclaration()
    {
        try (TestTable table = new TestTable(getQueryRunner()::execute, "test_supports_delete", "AS SELECT * FROM region")) {
            assertQueryFails("DELETE FROM " + table.getName(), "Deletes are not supported on the gs filesystem");
        }
    }

    @Override
    @Test
    public void verifySupportsRowLevelDeleteDeclaration()
    {
        try (TestTable table = new TestTable(getQueryRunner()::execute, "test_supports_row_level_delete", "AS SELECT * FROM region")) {
            assertQueryFails("DELETE FROM " + table.getName() + " WHERE regionkey = 2", "Deletes are not supported on the gs filesystem");
        }
    }

    // Override required because inserts on GCS are not supported, however CTAS is
    @Override
    @Test
    public void testCreatePartitionedTable()
    {
        String tableName = "test_create_partitioned_table_" + randomTableSuffix();
        assertUpdate("CREATE TABLE " + tableName + " (a, b, c) " +
                "WITH (location = '" + getLocationForTable(bucketName, tableName) + "', partitioned_by = ARRAY['b']) " +
                "AS VALUES (1, 'a', TIMESTAMP '2020-01-01 01:22:34.000 UTC'), (2, 'b', TIMESTAMP '2021-01-01 01:22:34.000 UTC')", 2);
        assertQuery("SELECT a, b, CAST(c AS VARCHAR) FROM " + tableName, "VALUES (1, 'a', '2020-01-01 01:22:34.000 UTC'), (2, 'b', '2021-01-01 01:22:34.000 UTC')");
        assertUpdate("DROP TABLE " + tableName);
    }
}
