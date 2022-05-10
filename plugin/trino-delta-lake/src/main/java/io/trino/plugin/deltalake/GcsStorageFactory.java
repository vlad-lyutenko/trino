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

import com.google.api.client.auth.oauth2.Credential;
import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.storage.Storage;
import com.google.cloud.hadoop.util.CredentialFactory;
import com.google.cloud.hadoop.util.HttpTransportFactory;
import com.google.cloud.hadoop.util.RetryHttpInitializer;
import io.trino.plugin.hive.gcs.HiveGcsConfig;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorSession;

import javax.inject.Inject;

import static com.google.cloud.hadoop.util.HttpTransportFactory.DEFAULT_TRANSPORT_TYPE;
import static com.google.common.base.Strings.nullToEmpty;
import static io.trino.plugin.hive.gcs.GcsAccessTokenProvider.EXPIRATION_TIME_MILLISECONDS;
import static io.trino.plugin.hive.gcs.GcsConfigurationProvider.GCS_OAUTH_KEY;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;

public class GcsStorageFactory
{
    // JSON factory used for formatting GCS JSON API payloads.
    private static final JsonFactory JSON_FACTORY = new JacksonFactory();

    private static final String APPLICATION_NAME = "Trino";

    private final boolean useGcsAccessToken;
    private final String jsonKeyFilePath;

    @Inject
    public GcsStorageFactory(HiveGcsConfig hiveGcsConfig)
    {
        this.useGcsAccessToken = hiveGcsConfig.isUseGcsAccessToken();
        this.jsonKeyFilePath = hiveGcsConfig.getJsonKeyFilePath();
    }

    public Storage create(ConnectorSession session)
    {
        try {
            HttpTransport httpTransport = HttpTransportFactory.createHttpTransport(DEFAULT_TRANSPORT_TYPE);
            Credential credential;
            if (useGcsAccessToken) {
                String accessToken = nullToEmpty(session.getIdentity().getExtraCredentials().get(GCS_OAUTH_KEY));
                credential = new PassthroughGoogleCredential(accessToken).createScoped(CredentialFactory.GCS_SCOPES);
            }
            else {
                credential = new CredentialFactory().getCredentialFromJsonKeyFile(jsonKeyFilePath, CredentialFactory.GCS_SCOPES, httpTransport);
            }
            return new Storage.Builder(httpTransport, JSON_FACTORY, new RetryHttpInitializer(credential, APPLICATION_NAME))
                    .setApplicationName(APPLICATION_NAME)
                    .build();
        }
        catch (Exception e) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, e);
        }
    }

    private static final class PassthroughGoogleCredential
            extends GoogleCredential
    {
        public PassthroughGoogleCredential(String accessToken)
        {
            setAccessToken(accessToken);
            setExpirationTimeMilliseconds(EXPIRATION_TIME_MILLISECONDS);
        }
    }
}
