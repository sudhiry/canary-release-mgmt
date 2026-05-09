import { Admin, Kafka, type GroupDescription } from "kafkajs";

const BROKER = process.env.KAFKA_BOOTSTRAP_SERVER ?? "localhost:9092";

let kafka: Kafka | null = null;
let admin: Admin | null = null;

export async function connect(): Promise<void> {
  if (admin) return;
  kafka = new Kafka({ clientId: "e2e-admin", brokers: [BROKER] });
  admin = kafka.admin();
  await admin.connect();
}

export async function disconnect(): Promise<void> {
  if (admin) {
    await admin.disconnect();
    admin = null;
  }
  kafka = null;
}

export async function consumerGroupMembers(groupId: string): Promise<GroupDescription> {
  if (!admin) throw new Error("kafka-admin: connect() must be called first");
  const desc = await admin.describeGroups([groupId]);
  if (desc.groups.length === 0) throw new Error(`kafka-admin: group not found: ${groupId}`);
  return desc.groups[0];
}

export async function listConsumerGroups(): Promise<string[]> {
  if (!admin) throw new Error("kafka-admin: connect() must be called first");
  const r = await admin.listGroups();
  return r.groups.map((g) => g.groupId);
}
