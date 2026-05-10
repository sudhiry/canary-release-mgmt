// Local shape replacing the removed Notification interface from restate-defs-node.
export interface StoredNotification {
  id: string;
  userId: string;
  message: string;
  status: string;
}

export interface ConsumedEvent {
  topic: string;
  key: string | null;
  value: string;
  headers: Record<string, string>;
}

class NotificationStore {
  public byId = new Map<string, StoredNotification>();

  put(n: StoredNotification): void {
    this.byId.set(n.id, n);
  }

  byUserId(userId: string): StoredNotification[] {
    return Array.from(this.byId.values()).filter((n) => n.userId === userId);
  }

  all(): StoredNotification[] {
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
