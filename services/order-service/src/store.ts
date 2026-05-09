import type { Order } from "@canary/restate-defs-node";

export interface ConsumedEvent {
  topic: string;
  key: string | null;
  value: string;
  headers: Record<string, string>;
}

class OrderStore {
  private byId = new Map<string, Order>();

  put(o: Order): void {
    this.byId.set(o.id, o);
  }

  findById(id: string): Order | undefined {
    return this.byId.get(id);
  }
}

class ConsumedEventStore {
  private events: ConsumedEvent[] = [];

  record(e: ConsumedEvent): void {
    this.events.push(e);
  }

  all(): ConsumedEvent[] {
    return [...this.events];
  }
}

export const orderStore = new OrderStore();
export const consumedEventStore = new ConsumedEventStore();
