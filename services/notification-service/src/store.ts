import type { Notification } from "@canary/restate-defs-node";

export interface ConsumedEvent {
  topic: string;
  key: string | null;
  value: string;
  headers: Record<string, string>;
}

class NotificationStore {
  private byId = new Map<string, Notification>();

  put(n: Notification): void {
    this.byId.set(n.id, n);
  }

  byUserId(userId: string): Notification[] {
    return Array.from(this.byId.values()).filter((n) => n.userId === userId);
  }

  all(): Notification[] {
    return Array.from(this.byId.values());
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

export const notificationStore = new NotificationStore();
export const consumedEventStore = new ConsumedEventStore();
