export function resolveConsumerGroupId(baseGroupId: string): string {
  if (typeof baseGroupId !== "string" || baseGroupId.trim().length === 0) {
    throw new Error("baseGroupId must not be blank");
  }
  const raw = process.env.VERSION;
  const version = raw && raw.trim().length > 0 ? raw.trim() : "stable";
  return `${baseGroupId}-${version}`;
}
