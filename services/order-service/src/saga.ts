import type { AxiosInstance } from "axios";
import type {
  OrderRequest,
  Reservation,
  ReservationRequest,
  Charge,
  ChargeRequest,
  Notification,
  NotifyRequest,
} from "@canary/restate-defs-node";

export interface SagaClients {
  inventory: AxiosInstance;
  payment: AxiosInstance;
  notification: AxiosInstance;
}

export interface SagaResult {
  reservation: Reservation;
  charge: Charge;
  notification: Notification;
}

/**
 * Sequential HTTP fan-out (saga without compensation per trimmed-C). Phase 3
 * will move this inside the CheckoutSaga workflow with proper compensation.
 *
 * Each axios client is configured with attachXCanaryAxiosInterceptor by the
 * caller (see http.ts) so x-canary propagates automatically.
 */
export async function runSaga(orderId: string, req: OrderRequest, clients: SagaClients): Promise<SagaResult> {
  const reservationReq: ReservationRequest = {
    sku: req.sku,
    quantity: req.quantity,
    orderId,
  };
  const reservation = (await clients.inventory.post<Reservation>("/reservations", reservationReq)).data;

  const chargeReq: ChargeRequest = { orderId, amount: req.amount };
  const charge = (await clients.payment.post<Charge>("/charges", chargeReq)).data;

  const notifyReq: NotifyRequest = {
    userId: req.userId,
    message: `Order ${orderId} confirmed`,
    orderId,
  };
  const notification = (await clients.notification.post<Notification>("/notifications", notifyReq)).data;

  return { reservation, charge, notification };
}
