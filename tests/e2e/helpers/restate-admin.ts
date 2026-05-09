import axios from "axios";

const ADMIN_URL = process.env.RESTATE_ADMIN_URL ?? "http://localhost:9070";

export interface RestateDeployment {
  id: string;
  uri?: string;
  services: Array<{ name: string; revision: number }>;
}

export async function listDeployments(): Promise<RestateDeployment[]> {
  const r = await axios.get(`${ADMIN_URL}/deployments`, { validateStatus: () => true });
  if (r.status !== 200) {
    throw new Error(`restate-admin: GET /deployments returned ${r.status}: ${JSON.stringify(r.data)}`);
  }
  const data = r.data as { deployments?: RestateDeployment[] };
  return data.deployments ?? [];
}

export async function listServices(): Promise<string[]> {
  const r = await axios.get(`${ADMIN_URL}/services`, { validateStatus: () => true });
  if (r.status !== 200) {
    throw new Error(`restate-admin: GET /services returned ${r.status}: ${JSON.stringify(r.data)}`);
  }
  const data = r.data as { services?: Array<{ name: string }> };
  return (data.services ?? []).map((s) => s.name);
}
