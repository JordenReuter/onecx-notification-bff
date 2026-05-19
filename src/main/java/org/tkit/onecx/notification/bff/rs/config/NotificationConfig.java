package org.tkit.onecx.notification.bff.rs.config;

import io.quarkus.runtime.annotations.ConfigDocFilename;
import io.quarkus.runtime.annotations.ConfigPhase;
import io.quarkus.runtime.annotations.ConfigRoot;
import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithDefault;
import io.smallrye.config.WithName;

@ConfigDocFilename("onecx-notification-bff.adoc")
@ConfigMapping(prefix = "onecx.notification")
@ConfigRoot(phase = ConfigPhase.RUN_TIME)
public interface NotificationConfig {

    /**
     * Token configuration.
     *
     * @return token configuration.
     */
    @WithName("token")
    TokenConfig tokenConfig();

    /**
     * Websocket configuration.
     *
     * @return websocket configuration.
     */
    @WithName("websocket")
    WebsocketConfig websocketConfig();

    /**
     * Token configuration.
     */
    interface TokenConfig {
        /**
         * Verified access token
         */
        @WithName("verified")
        boolean verified();

        /**
         * Issuer public key location suffix.
         */
        @WithName("issuer.public-key-location.suffix")
        @WithDefault("/protocol/openid-connect/certs")
        String publicKeyLocationSuffix();

        /**
         * Issuer public key location enabled
         */
        @WithName("issuer.public-key-location.enabled")
        boolean publicKeyEnabled();

    }

    /**
     * Websocket configuration.
     */
    interface WebsocketConfig {
        /**
         * Websocket heartbeat interval in ms.
         */
        @WithName("heartbeat-interval")
        @WithDefault("25000")
        String heartbeatInterval();

        /**
         * Websocket session timeout in ms.
         */
        @WithName("session-timeout")
        @WithDefault("30000")
        String sessionTimeout();

        /**
         * Websocket ping/pong timeout in ms.
         */
        @WithName("ping-timeout")
        @WithDefault("60000")
        String pingTimeout();
    }
}
