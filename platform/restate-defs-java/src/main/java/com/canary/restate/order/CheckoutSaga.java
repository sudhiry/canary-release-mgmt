package com.canary.restate.order;

import dev.restate.sdk.WorkflowContext;
import dev.restate.sdk.annotation.Handler;
import dev.restate.sdk.annotation.Workflow;

@Workflow
public abstract class CheckoutSaga {
    @Handler
    public abstract Order run(WorkflowContext ctx, OrderRequest req);
}
