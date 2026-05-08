package com.canary.restate.notification;

import dev.restate.sdk.Context;
import dev.restate.sdk.annotation.Handler;
import dev.restate.sdk.annotation.Service;

@Service
public abstract class NotificationService {
    @Handler
    public abstract Notification notify(Context ctx, NotifyRequest req);
}
