import * as restate from "@restatedev/restate-sdk";

// ----- audit -----
export interface AuditEvent {
  aggregate: string;
  id: string;
  action: string;
  correlationId?: string;
}

export type AuditQueryServiceMethods = {
  append(event: AuditEvent): Promise<void>;
  byAggregate(aggregateId: string): Promise<AuditEvent[]>;
};

// audit is not subset-forked; one definition.
export const auditQueryServiceDef = {
  name: "AuditQueryService",
} as restate.ServiceDefinitionFrom<AuditQueryServiceMethods>;

// ----- payment -----
export interface ChargeRequest {
  orderId: string;
  amount: number;
}

export interface Charge {
  id: string;
  orderId: string;
  amount: number;     // canary: (req.amount * 99) / 100; stable: req.amount
  status: string;
}

export type PaymentVOMethods = {
  charge(req: ChargeRequest): Promise<Charge>;
  refund(req: ChargeRequest): Promise<Charge>;
};

export const paymentVOStableDef = {
  name: "PaymentVOStable",
} as restate.VirtualObjectDefinitionFrom<PaymentVOMethods>;

export const paymentVOCanaryDef = {
  name: "PaymentVOCanary",
} as restate.VirtualObjectDefinitionFrom<PaymentVOMethods>;

// ----- inventory -----
export interface ReservationRequest {
  sku: string;
  quantity: number;
  orderId: string;
}

export interface Reservation {
  id: string;
  sku: string;
  quantity: number;
  orderId: string;
  status: string;
  bufferUnits: number;   // 0 stable, 1 canary
}

export interface AvailabilityResponse {
  sku: string;
  available: number;
}

export type ReservationWorkflowMethods = {
  run(req: ReservationRequest): Promise<Reservation>;
  confirm(): Promise<Reservation>;   // was Promise<void>; now returns confirmed Reservation
  release(): Promise<void>;
};

export const reservationWorkflowStableDef = {
  name: "ReservationWorkflowStable",
} as restate.WorkflowDefinitionFrom<ReservationWorkflowMethods>;

export const reservationWorkflowCanaryDef = {
  name: "ReservationWorkflowCanary",
} as restate.WorkflowDefinitionFrom<ReservationWorkflowMethods>;

// ----- notification -----
export interface NotifyRequest {
  userId: string;
  message: string;
  orderId: string;
}

export interface NotifyResult {
  delivered: boolean;
  version: "stable" | "canary";
  deliveredMessage: string;   // canary appends "[via canary notifier]"
}

export type NotificationServiceMethods = {
  notify(req: NotifyRequest): Promise<NotifyResult>;
};

export const notificationServiceStableDef = {
  name: "NotificationServiceStable",
} as restate.ServiceDefinitionFrom<NotificationServiceMethods>;

export const notificationServiceCanaryDef = {
  name: "NotificationServiceCanary",
} as restate.ServiceDefinitionFrom<NotificationServiceMethods>;

// ----- order -----
export interface OrderRequest {
  userId: string;
  sku: string;
  quantity: number;
  amount: number;
}

export interface Order {
  id: string;
  userId: string;
  sku: string;
  quantity: number;
  amount: number;
  status: string;
  auditTrail: string[];   // NEW: per-hop "<svc>@<variant>" entries
}

export type CheckoutSagaMethods = {
  run(req: OrderRequest): Promise<Order>;
};

export const checkoutSagaStableDef = {
  name: "CheckoutSagaStable",
} as restate.WorkflowDefinitionFrom<CheckoutSagaMethods>;

export const checkoutSagaCanaryDef = {
  name: "CheckoutSagaCanary",
} as restate.WorkflowDefinitionFrom<CheckoutSagaMethods>;
