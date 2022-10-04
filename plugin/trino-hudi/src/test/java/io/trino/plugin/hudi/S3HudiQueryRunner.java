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
package io.trino.plugin.hudi;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.log.Logger;
import io.airlift.log.Logging;
import io.trino.hdfs.DynamicHdfsConfiguration;
import io.trino.hdfs.HdfsConfig;
import io.trino.hdfs.HdfsConfigurationInitializer;
import io.trino.hdfs.HdfsContext;
import io.trino.hdfs.HdfsEnvironment;
import io.trino.hdfs.authentication.NoHdfsAuthentication;
import io.trino.plugin.hive.SchemaAlreadyExistsException;
import io.trino.plugin.hive.containers.HiveMinioDataLake;
import io.trino.plugin.hive.metastore.Database;
import io.trino.plugin.hive.metastore.HiveMetastore;
import io.trino.plugin.hive.metastore.thrift.BridgingHiveMetastore;
import io.trino.plugin.hive.s3.HiveS3Config;
import io.trino.plugin.hive.s3.TrinoS3ConfigurationInitializer;
import io.trino.plugin.hudi.testing.HudiTablesInitializer;
import io.trino.plugin.hudi.testing.TpchHudiTablesInitializer;
import io.trino.spi.connector.CatalogSchemaName;
import io.trino.spi.security.PrincipalType;
import io.trino.testing.DistributedQueryRunner;
import io.trino.tpch.TpchTable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import java.util.Map;
import java.util.Optional;

import static io.trino.plugin.hive.HiveTestUtils.SOCKS_PROXY;
import static io.trino.plugin.hive.TestingThriftHiveMetastoreBuilder.testingThriftHiveMetastoreBuilder;
import static io.trino.plugin.hive.containers.HiveMinioDataLake.MINIO_ACCESS_KEY;
import static io.trino.plugin.hive.containers.HiveMinioDataLake.MINIO_SECRET_KEY;
import static io.trino.testing.DistributedQueryRunner.builder;
import static io.trino.testing.TestingConnectorSession.SESSION;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static org.apache.hudi.common.model.HoodieTableType.COPY_ON_WRITE;

public final class S3HudiQueryRunner
{
    static final CatalogSchemaName HUDI_MINIO_TESTS = new CatalogSchemaName("hudi", "miniotests");
    private static final HdfsContext CONTEXT = new HdfsContext(SESSION);

    private S3HudiQueryRunner() {}

    public static DistributedQueryRunner create(
            String catalogName,
            String schemaName,
            Map<String, String> connectorProperties,
            HudiTablesInitializer dataLoader,
            HiveMinioDataLake hiveMinioDataLake)
            throws Exception
    {
        return create(
                catalogName,
                schemaName,
                ImmutableMap.of(),
                connectorProperties,
                dataLoader,
                hiveMinioDataLake);
    }

    public static DistributedQueryRunner create(
            String catalogName,
            String schemaName,
            Map<String, String> extraProperties,
            Map<String, String> connectorProperties,
            HudiTablesInitializer dataLoader,
            HiveMinioDataLake hiveMinioDataLake)
            throws Exception
    {
        String basePath = "s3a://" + hiveMinioDataLake.getBucketName() + "/" + schemaName;
        HdfsEnvironment hdfsEnvironment = getHdfsEnvironment(hiveMinioDataLake);
        Configuration configuration = hdfsEnvironment.getConfiguration(CONTEXT, new Path(basePath));

        HiveMetastore metastore = new BridgingHiveMetastore(
                testingThriftHiveMetastoreBuilder()
                        .metastoreClient(hiveMinioDataLake.getHiveHadoop().getHiveMetastoreEndpoint())
                        .hdfsEnvironment(hdfsEnvironment)
                        .build());
        Database database = Database.builder()
                .setDatabaseName(schemaName)
                .setOwnerName(Optional.of("public"))
                .setOwnerType(Optional.of(PrincipalType.ROLE))
                .build();
        try {
            metastore.createDatabase(database);
        }
        catch (SchemaAlreadyExistsException e) {
            // do nothing if database already exists
        }

        DistributedQueryRunner queryRunner = builder(
                testSessionBuilder()
                        .setCatalog(catalogName)
                        .setSchema(schemaName)
                        .build())
                .setExtraProperties(extraProperties)
                .build();
        queryRunner.installPlugin(new TestingHudiPlugin(Optional.of(metastore)));
        queryRunner.createCatalog(
                catalogName,
                "hudi",
                ImmutableMap.<String, String>builder()
                        .put("hive.s3.aws-access-key", MINIO_ACCESS_KEY)
                        .put("hive.s3.aws-secret-key", MINIO_SECRET_KEY)
                        .put("hive.s3.endpoint", hiveMinioDataLake.getMinioAddress())
                        .put("hive.s3.path-style-access", "true")
                        .putAll(connectorProperties)
                        .buildOrThrow());

        dataLoader.initializeTables(queryRunner, metastore, HUDI_MINIO_TESTS, basePath, configuration);
        return queryRunner;
    }

    private static HdfsEnvironment getHdfsEnvironment(HiveMinioDataLake hiveMinioDataLake)
    {
        DynamicHdfsConfiguration dynamicHdfsConfiguration = new DynamicHdfsConfiguration(
                new HdfsConfigurationInitializer(
                        new HdfsConfig()
                                .setSocksProxy(SOCKS_PROXY.orElse(null)),
                        ImmutableSet.of(
                                new TrinoS3ConfigurationInitializer(new HiveS3Config()
                                        .setS3AwsAccessKey(MINIO_ACCESS_KEY)
                                        .setS3AwsSecretKey(MINIO_SECRET_KEY)
                                        .setS3Endpoint(hiveMinioDataLake.getMinioAddress())
                                        .setS3PathStyleAccess(true)))),
                ImmutableSet.of());

        return new HdfsEnvironment(
                dynamicHdfsConfiguration,
                new HdfsConfig(),
                new NoHdfsAuthentication());
    }

    public static void main(String[] args)
            throws Exception
    {
        Logging.initialize();
        Logger log = Logger.get(S3HudiQueryRunner.class);

        String bucketName = "test-bucket";
        HiveMinioDataLake hiveMinioDataLake = new HiveMinioDataLake(bucketName);
        hiveMinioDataLake.start();
        /*
         * Please set the below VM arguments for the main method to run:
         *
         * -ea --add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.util=ALL-UNNAMED --add-opens=java.base/java.util.concurrent=ALL-UNNAMED
         *
         * TODO: We need to set above arguments due to Hudi's deep reflection based ObjectSizeEstimator.
         *       Higher versions of jdk block illegal reflective access. This will not be needed after
         *       https://issues.apache.org/jira/browse/HUDI-4687 is fixed.
         */
        DistributedQueryRunner queryRunner = create(
                HUDI_MINIO_TESTS.getCatalogName(),
                HUDI_MINIO_TESTS.getSchemaName(),
                ImmutableMap.of("http-server.http.port", "8080"),
                ImmutableMap.of(),
                new TpchHudiTablesInitializer(COPY_ON_WRITE, TpchTable.getTables()),
                hiveMinioDataLake);

        log.info("======== SERVER STARTED ========");
        log.info("\n====\n%s\n====", queryRunner.getCoordinator().getBaseUrl());
    }
}
