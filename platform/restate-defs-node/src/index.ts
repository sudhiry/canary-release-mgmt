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
  amount: number;
  status: string;
}

export type PaymentVOMethods = {
  charge(req: ChargeRequest): Promise<Charge>;
};

export const paymentVODef = {
  name: "PaymentVO",
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
}

export interface AvailabilityResponse {
  sku: string;
  available: number;
}

export type ReservationWorkflowMethods = {
  run(req: ReservationRequest): Promise<Reservation>;
};

export const reservationWorkflowDef = {
  name: "ReservationWorkflow",
} as restate.WorkflowDefinitionFrom<ReservationWorkflowMethods>;

// ----- notification -----
export interface NotifyRequest {
  userId: string;
  message: string;
  orderId: string;
}

export interface Notification {
  id: string;
  userId: string;
  message: string;
  status: string;
}

export type NotificationServiceMethods = {
  notify(req: NotifyRequest): Promise<Notification>;
};

export const notificationServiceDef = {
  name: "NotificationService",
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
}

export type CheckoutSagaMethods = {
  run(req: OrderRequest): Promise<Order>;
};

export const checkoutSagaDef = {
  name: "CheckoutSaga",
} as restate.WorkflowDefinitionFrom<CheckoutSagaMethods>;
