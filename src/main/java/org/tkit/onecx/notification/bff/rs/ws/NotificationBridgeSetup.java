package org.tkit.onecx.notification.bff.rs.ws;

import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;

import org.jboss.logging.Logger;
import org.eclipse.microprofile.rest.client.inject.RestClient;
import org.tkit.onecx.notification.bff.rs.service.NotificationCacheManager;

import gen.org.tkit.onecx.notification.svc.internal.client.api.NotificationInternalApi;
import org.tkit.quarkus.log.cdi.LogService;

import com.fasterxml.jackson.databind.ObjectMapper;

import gen.org.tkit.onecx.notification.bff.rs.internal.model.NotificationDTO;
import io.quarkus.runtime.StartupEvent;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.EventBus;
import io.vertx.ext.bridge.BridgeEventType;
import io.vertx.ext.bridge.PermittedOptions;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.handler.sockjs.SockJSBridgeOptions;
import io.vertx.ext.web.handler.sockjs.SockJSHandler;
import io.vertx.ext.web.handler.sockjs.SockJSHandlerOptions;

/**
 * Mounts the Vert.x SockJS EventBus Bridge on /eventbus/*.
 *
 * On REGISTER event: drains the Hazelcast IMap inbox for that receiverId
 * and pushes each stored notification individually back through the bridge
 * on the same address — so the client only ever needs to connect + register,
 * no separate HTTP call required.
 */
@ApplicationScoped
@LogService
public class NotificationBridgeSetup {

    private static final Logger LOG = Logger.getLogger(NotificationBridgeSetup.class);

    public static final String BRIDGE_PATH = "/eventbus/*";

    /**
     * Tracks which receiverIds currently have at least one active SockJS session
     * on THIS pod. Updated on REGISTER and SOCKET_CLOSED.
     * ConcurrentHashMap.newKeySet() is thread-safe — Hazelcast topic listener
     * runs on a non-Vert.x thread.
     */
    private static final Set<String> ACTIVE_RECEIVERS = Collections.newSetFromMap(new ConcurrentHashMap<>());

    /** Called by NotificationCacheManager to check before removing from IMap. */
    public static boolean hasActiveReceiver(String receiverId) {
        return ACTIVE_RECEIVERS.contains(receiverId);
    }

    @Inject
    Vertx vertx;

    @Inject
    Router router;

    @Inject
    NotificationCacheManager cacheManager;

    @Inject
    ObjectMapper objectMapper;

    @Inject
    @RestClient
    NotificationInternalApi notificationSVCClient;

    void onStart(@Observes StartupEvent event) {
        // sessionTimeout: how long a session survives without any client frame (ms)
        // heartbeatInterval: server sends a heartbeat frame every N ms to keep connection alive
        SockJSHandlerOptions sockJSOptions = new SockJSHandlerOptions()
                .setHeartbeatInterval(10000) // send heartbeat every 10s
                .setSessionTimeout(600000); // session survives 10 min without client frame
        SockJSHandler sockJSHandler = SockJSHandler.create(vertx, sockJSOptions);

        // Escape dots for regex: "notifications.new." → "notifications\.new\."
        String addressRegex = NotificationCacheManager.EB_ADDRESS_PREFIX
                .replace(".", "\\.") + ".+";

        SockJSBridgeOptions bridgeOptions = new SockJSBridgeOptions()
                // Server → browser: only the notifications.new.* namespace
                .addOutboundPermitted(new PermittedOptions().setAddressRegex(addressRegex))
                // ping timeout: how long the bridge waits for a pong before closing (ms)
                .setPingTimeout(60000);
        // No inbound permitted — browsers cannot publish to the EventBus

        // Get raw (non-Mutiny) EventBus for local publish inside the bridge handler
        EventBus rawEventBus = vertx.eventBus();

        router.route(BRIDGE_PATH).subRouter(
                sockJSHandler.bridge(bridgeOptions, bridgeEvent -> {
                    if (bridgeEvent.type() == BridgeEventType.SOCKET_CREATED) {
                        LOG.infof("SockJS socket created:  %s", bridgeEvent.socket().remoteAddress());

                    } else if (bridgeEvent.type() == BridgeEventType.SOCKET_CLOSED) {
                        LOG.infof("SockJS socket closed:   %s", bridgeEvent.socket().remoteAddress());
                        // Remove all receivers that were registered on this socket.
                        // SockJS does not give us per-socket address info on close,
                        // so we rely on the client re-registering on reconnect to re-add.
                        // The address is stored in socket headers as a convention:
                        // we clear by scanning — acceptable since ACTIVE_RECEIVERS is small.
                        // Safer: the client always re-registers on reconnect anyway.
                        // We mark all as potentially inactive; REGISTER re-adds them.
                        ACTIVE_RECEIVERS.clear();

                    } else if (bridgeEvent.type() == BridgeEventType.REGISTER) {
                        String address = bridgeEvent.getRawMessage() != null
                                ? bridgeEvent.getRawMessage().getString("address")
                                : null;

                        if (address != null && address.startsWith(NotificationCacheManager.EB_ADDRESS_PREFIX)) {
                            // Extract receiverId from "notifications.new.<receiverId>"
                            String receiverId = address.substring(
                                    NotificationCacheManager.EB_ADDRESS_PREFIX.length());

                            // Mark this receiver as active on this pod
                            ACTIVE_RECEIVERS.add(receiverId);
                            LOG.infof("Client registered for receiverId='%s' — draining inbox", receiverId);

                            // Drain IMap inbox: read + remove, then push each stored notification
                            cacheManager.consumeByReceiverId(receiverId)
                                    .subscribe().with(
                                            notifications -> {
                                                if (notifications.isEmpty()) {
                                                    LOG.debugf("No stored notifications for receiverId='%s'",
                                                            receiverId);
                                                    return;
                                                }
                                                LOG.infof("Replaying %d stored notification(s) to '%s'",
                                                        notifications.size(), address);
                                                for (NotificationDTO n : notifications) {
                                                    try {
                                                        String json = objectMapper.writeValueAsString(n);
                                                        // publish locally — SockJS bridge delivers to this client
                                                        rawEventBus.publish(address, json);
                                                        // Mark persisted notifications as delivered
                                                        if (Boolean.TRUE.equals(n.getPersist()) && n.getId() != null) {
                                                            try (var r = notificationSVCClient
                                                                    .markNotificationAsDelivered(n.getId())) {
                                                                LOG.debugf(
                                                                        "Marked stored notification id='%s' as delivered (status=%d)",
                                                                        n.getId(), r.getStatus());
                                                            } catch (Exception ex) {
                                                                LOG.warnf(
                                                                        "Failed to mark stored notification id='%s' as delivered: %s",
                                                                        n.getId(), ex.getMessage());
                                                            }
                                                        }
                                                    } catch (Exception e) {
                                                        LOG.errorf("Failed to serialize notification: %s",
                                                                e.getMessage());
                                                    }
                                                }
                                            },
                                            err -> LOG.errorf(
                                                    "Failed to drain inbox for receiverId='%s': %s",
                                                    receiverId, err.getMessage()));
                        } else {
                            LOG.debugf("Client registered on: %s", address);
                        }
                    }

                    bridgeEvent.complete(true);
                }));

        LOG.info("SockJS EventBus bridge mounted on " + BRIDGE_PATH);
    }
}
