package org.tkit.onecx.notification.bff.rs.controllers;

import java.util.List;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.transaction.Transactional;
import jakarta.ws.rs.core.Response;

import org.tkit.onecx.notification.bff.rs.mappers.NotificationMapper;
import org.tkit.onecx.notification.bff.rs.service.NotificationCacheManager;
import org.tkit.quarkus.log.cdi.LogService;

import gen.org.tkit.onecx.notification.bff.rs.internal.NotificationInternalApiService;
import gen.org.tkit.onecx.notification.bff.rs.internal.model.NotificationDTO;
import gen.org.tkit.onecx.notification.bff.rs.internal.model.NotificationRetrieveRequestDTO;
import gen.org.tkit.onecx.notification.bff.rs.internal.model.NotificationRetrieveResponseDTO;

@ApplicationScoped
@Transactional(value = Transactional.TxType.NOT_SUPPORTED)
@LogService
public class NotificationRestController implements NotificationInternalApiService {

    @Inject
    NotificationCacheManager cacheManager;

    @Inject
    NotificationMapper mapper;

    @Override
    public Response dispatchNotification(NotificationDTO notificationDTO) {
        cacheManager.storeNotification(notificationDTO).await().indefinitely();
        return Response.ok().build();
    }

    @Override
    public Response retrieveNotifications(NotificationRetrieveRequestDTO notificationRetrieveRequestDTO) {
        List<NotificationDTO> notifications = cacheManager
                .consumeNotifications(notificationRetrieveRequestDTO)
                .await().indefinitely();
        NotificationRetrieveResponseDTO response = mapper.toResponse(notifications);
        return Response.ok(response).build();
    }
}
