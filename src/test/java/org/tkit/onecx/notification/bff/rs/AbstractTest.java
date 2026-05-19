package org.tkit.onecx.notification.bff.rs;

import java.nio.charset.StandardCharsets;
import java.util.Base64;

import org.eclipse.microprofile.config.ConfigProvider;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import io.quarkiverse.mockserver.test.MockServerTestResource;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.keycloak.client.KeycloakTestClient;
import io.restassured.RestAssured;
import io.restassured.config.ObjectMapperConfig;
import io.restassured.config.RestAssuredConfig;
import io.vertx.core.json.JsonObject;

@QuarkusTestResource(MockServerTestResource.class)
@QuarkusTestResource(HazelcastTestResource.class)
public abstract class AbstractTest {
    protected static final String ADMIN = "alice";

    protected static final String USER = "bob";

    protected static final String APM_HEADER_PARAM = ConfigProvider.getConfig()
            .getOptionalValue("tkit.rs.context.token.header-param", String.class)
            .orElse("apm-principal-token");
    KeycloakTestClient keycloakClient = new KeycloakTestClient();

    protected String getKeycloakClientToken(String clientId) {
        return keycloakClient.getClientAccessToken(clientId);
    }

    protected String getKeycloakUserToken(String userName) {
        return keycloakClient.getAccessToken(userName);
    }

    protected String getTokenSubject(String token) {
        String payload = token.split("\\.")[1];
        String json = new String(Base64.getUrlDecoder().decode(payload), StandardCharsets.UTF_8);
        return new JsonObject(json).getString("sub");
    }

    static {
        RestAssured.config = RestAssuredConfig.config().objectMapperConfig(
                ObjectMapperConfig.objectMapperConfig().jackson2ObjectMapperFactory(
                        (cls, charset) -> {
                            ObjectMapper objectMapper = new ObjectMapper();
                            objectMapper.registerModule(new JavaTimeModule());
                            objectMapper.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);
                            return objectMapper;
                        }));
    }
}
