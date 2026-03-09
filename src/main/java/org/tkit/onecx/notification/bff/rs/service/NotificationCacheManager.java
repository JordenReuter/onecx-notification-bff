package org.tkit.onecx.notification.bff.rs.service;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;

import org.eclipse.microprofile.rest.client.inject.RestClient;
import org.jboss.logging.Logger;
import org.tkit.onecx.notification.bff.rs.ws.NotificationBridgeSetup;
import org.tkit.quarkus.log.cdi.LogService;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.topic.ITopic;

import gen.org.tkit.onecx.notification.bff.rs.internal.model.NotificationDTO;
import gen.org.tkit.onecx.notification.bff.rs.internal.model.NotificationRetrieveRequestDTO;
import gen.org.tkit.onecx.notification.svc.internal.client.api.NotificationInternalApi;
import io.quarkus.runtime.StartupEvent;
import io.smallrye.mutiny.Uni;
import io.vertx.core.json.JsonObject;
import io.vertx.mutiny.core.Vertx;
import io.vertx.mutiny.core.eventbus.EventBus;
import io.vertx.mutiny.core.shareddata.AsyncMap;

/**
 * Manages the cluster-wide notification cache backed by a Hazelcast IMap.
 *
 * Cross-pod fan-out strategy:
 * Uses a Hazelcast ITopic ("notifications.topic") which is a true pub/sub:
 * ALL subscribers on ALL cluster members receive every message.
 * Each pod subscribes at startup and re-publishes the payload locally
 * to the Vert.x EventBus so the SockJS bridge can forward it to
 * connected browser sessions on that pod.
 */
@ApplicationScoped
@LogService
public class NotificationCacheManager {

    public static final String MAP_NAME = "notifications";
    public static final String EB_ADDRESS_PREFIX = "notifications.new.";
    private static final String TOPIC_NAME = "notifications.topic";

    private static final Logger LOG = Logger.getLogger(NotificationCacheManager.class);

    @Inject
    Vertx vertx;

    @Inject
    EventBus eventBus;

    @Inject
    @RestClient
    NotificationInternalApi notificationSVCClient;

    private final AtomicReference<AsyncMap<String, List<NotificationDTO>>> clusterMapRef = new AtomicReference<>();
    private ITopic<String> topic;

    /**
     * At startup: get the Hazelcast ITopic and add a MessageListener.
     * The listener fires on EVERY pod when any pod publishes to the topic.
     * It then re-publishes locally to the Vert.x EventBus → SockJS bridge.
     */
    void onStart(@Observes StartupEvent ev) {
        // Hazelcast registers its instance globally — retrieve it without any casting or CDI tricks
        HazelcastInstance hz = Hazelcast.getAllHazelcastInstances().iterator().next();
        topic = hz.getTopic(TOPIC_NAME);
        topic.addMessageListener(message -> {
            JsonObject payload = new JsonObject(message.getMessageObject());
            String receiverId = payload.getString("receiverId");
            if (receiverId != null) {
                String localAddress = EB_ADDRESS_PREFIX + receiverId;
                LOG.debugf("Hazelcast topic → local EventBus publish to '%s'", localAddress);
                vertx.runOnContext(() -> {
                    eventBus.publish(localAddress, message.getMessageObject());
                    // Always remove from IMap after live delivery.
                    // - Client connected: receives live via SockJS, IMap cleared → no duplicate on reconnect ✅
                    // - Client offline: nobody is listening, IMap entry stays → drained on next REGISTER ✅
                    //
                    // Wait: this removes even when nobody received it. Solution: track active
                    // receivers in a local Set updated by the SockJS REGISTER/SOCKET_CLOSED events.
                    // The activeReceivers set is maintained by NotificationBridgeSetup.
                    if (NotificationBridgeSetup.hasActiveReceiver(receiverId)) {
                        Boolean persist = payload.getBoolean("persist");
                        String notificationId = payload.getString("id");
                        map().flatMap(m -> m.remove(receiverId))
                                .subscribe().with(
                                        ignored -> {
                                            LOG.debugf(
                                                    "Removed live-delivered notification from IMap key='%s'",
                                                    receiverId);
                                            if (Boolean.TRUE.equals(persist) && notificationId != null) {
                                                try (var r = notificationSVCClient
                                                        .markNotificationAsDelivered(notificationId)) {
                                                    LOG.debugf("Marked notification id='%s' as delivered (status=%d)",
                                                            notificationId, r.getStatus());
                                                } catch (Exception ex) {
                                                    LOG.warnf("Failed to mark notification id='%s' as delivered: %s",
                                                            notificationId, ex.getMessage());
                                                }
                                            }
                                        },
                                        err -> LOG.warnf(
                                                "Failed to remove live-delivered notification: %s",
                                                err.getMessage()));
                    }
                });
            }
        });

        LOG.info("Hazelcast topic listener registered on '" + TOPIC_NAME + "'");
    }

    // -------------------------------------------------------------------------
    // Internal: lazy map resolution
    // -------------------------------------------------------------------------

    private Uni<AsyncMap<String, List<NotificationDTO>>> map() {
        AsyncMap<String, List<NotificationDTO>> cached = clusterMapRef.get();
        if (cached != null) {
            return Uni.createFrom().item(cached);
        }
        return vertx.sharedData()
                .<String, List<NotificationDTO>> getClusterWideMap(MAP_NAME)
                .invoke(m -> {
                    clusterMapRef.compareAndSet(null, m);
                    LOG.info("Cluster-wide map '" + MAP_NAME + "' resolved and cached");
                });
    }

    // -------------------------------------------------------------------------
    // Public API
    // -------------------------------------------------------------------------

    public Uni<Void> storeNotification(NotificationDTO notification) {
        // Key is simply the receiverId — applicationId is part of the payload, not the key
        String key = notification.getReceiverId();

        return map().flatMap(m -> m.get(key)
                .map(existing -> existing != null ? existing : new ArrayList<NotificationDTO>())
                .flatMap(list -> {
                    list.add(notification);
                    return m.put(key, list);
                })
                .flatMap(ignored -> {
                    JsonObject payload = new JsonObject()
                            .put("id", notification.getId())
                            .put("applicationId", notification.getApplicationId())
                            .put("senderId", notification.getSenderId())
                            .put("receiverId", notification.getReceiverId())
                            .put("severity", notification.getSeverity() != null
                                    ? notification.getSeverity().name()
                                    : null)
                            .put("issuer", notification.getIssuer() != null
                                    ? notification.getIssuer().name()
                                    : null)
                            .put("persist", notification.getPersist());
                    if (notification.getContentMeta() != null && !notification.getContentMeta().isEmpty()) {
                        notification.getContentMeta().forEach(contentMetaDTO -> {
                            payload.put(contentMetaDTO.getKey(), contentMetaDTO.getValue());
                        });
                    }
                    // Publish to Hazelcast ITopic — true broadcast, ALL pods receive it
                    topic.publish(payload.encode());
                    LOG.debugf("Published to Hazelcast topic for receiverId='%s'",
                            notification.getReceiverId());
                    return Uni.createFrom().voidItem();
                }));
    }

    public Uni<List<NotificationDTO>> getNotifications(NotificationRetrieveRequestDTO request) {
        String receiverId = request.getReceiverId();

        if (receiverId == null || receiverId.isBlank()) {
            return Uni.createFrom().item(List.of());
        }

        return map().flatMap(m -> m.get(receiverId)
                .map(list -> list != null ? list : List.<NotificationDTO> of()));
    }

    public Uni<Void> clearNotifications(String receiverId) {
        return map().flatMap(m -> m.remove(receiverId))
                .replaceWithVoid();
    }

    /**
     * Consume-on-read: returns all notifications for the receiver and removes
     * them from the IMap in one step. This is the "inbox drain" operation —
     * call it on connect to get missed notifications; they will not be returned again.
     */
    public Uni<List<NotificationDTO>> consumeNotifications(NotificationRetrieveRequestDTO request) {
        return consumeByReceiverId(request.getReceiverId());
    }

    /**
     * Same as consumeNotifications but takes plain strings — used by the SockJS
     * bridge on REGISTER so no DTO construction is needed at the bridge layer.
     */
    public Uni<List<NotificationDTO>> consumeByReceiverId(String receiverId) {
        if (receiverId == null || receiverId.isBlank()) {
            return Uni.createFrom().item(List.of());
        }
        // Key is simply receiverId — remove and return in one step
        return map().flatMap(m -> m.remove(receiverId)
                .map(list -> list != null ? list : List.<NotificationDTO> of()));
    }
}
