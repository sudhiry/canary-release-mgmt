import { CoreV1Api, KubeConfig, type V1Endpoints, Watch } from "@kubernetes/client-node";
import { Gauge, type Registry, register as defaultRegister } from "prom-client";

const GAUGE_NAME = "canary_lane_active";

export class LaneStateProbe {
  private gauge?: Gauge<string>;
  private laneActive = new Map<string, number>([
    ["stable", 0],
    ["canary", 0],
  ]);
  private watchAbortControllers: AbortController[] = [];
  private closed = false;

  constructor(
    private readonly namespace: string,
    private readonly serviceName: string,
    private readonly registry: Registry = defaultRegister,
    private readonly kc: KubeConfig = LaneStateProbe.defaultKubeConfig(),
  ) {}

  registerGauges(): void {
    this.gauge = new Gauge({
      name: GAUGE_NAME,
      help: "1 when the lane has at least one ready endpoint, else 0.",
      labelNames: ["substrate", "service", "lane"],
      registers: [this.registry],
      collect: () => {
        if (!this.gauge) return;
        for (const [lane, value] of this.laneActive) {
          this.gauge.set({ substrate: "http", service: this.serviceName, lane }, value);
        }
      },
    });
    // Force an initial collect so the labels appear immediately.
    this.gauge.set({ substrate: "http", service: this.serviceName, lane: "stable" }, 0);
    this.gauge.set({ substrate: "http", service: this.serviceName, lane: "canary" }, 0);
  }

  setLaneActive(lane: "stable" | "canary", active: boolean): void {
    this.laneActive.set(lane, active ? 1 : 0);
    if (this.gauge) {
      this.gauge.set({ substrate: "http", service: this.serviceName, lane }, active ? 1 : 0);
    }
  }

  laneValue(lane: "stable" | "canary"): number {
    return this.laneActive.get(lane) ?? 0;
  }

  static hasAddresses(e: V1Endpoints | undefined): boolean {
    if (!e?.subsets) return false;
    return e.subsets.some((s) => Array.isArray(s.addresses) && s.addresses.length > 0);
  }

  async start(): Promise<void> {
    if (!this.gauge) this.registerGauges();
    await this.watchEndpoints(`${this.serviceName}-stable`, "stable");
    await this.watchEndpoints(`${this.serviceName}-canary`, "canary");
  }

  private async watchEndpoints(endpointsName: string, lane: "stable" | "canary"): Promise<void> {
    const coreApi = this.kc.makeApiClient(CoreV1Api);
    // Initial state
    try {
      const ep = await coreApi.readNamespacedEndpoints({ namespace: this.namespace, name: endpointsName });
      this.setLaneActive(lane, LaneStateProbe.hasAddresses(ep));
    } catch {
      this.setLaneActive(lane, false);
    }
    const watch = new Watch(this.kc);
    const ctrl = await watch.watch(
      `/api/v1/namespaces/${this.namespace}/endpoints`,
      { fieldSelector: `metadata.name=${endpointsName}` },
      (type: string, obj: V1Endpoints) => {
        const active = type !== "DELETED" && LaneStateProbe.hasAddresses(obj);
        this.setLaneActive(lane, active);
      },
      (_err) => {
        if (!this.closed) {
          // Best-effort reconnect after 1s
          setTimeout(() => { void this.watchEndpoints(endpointsName, lane); }, 1000);
        }
      },
    );
    this.watchAbortControllers.push(ctrl);
  }

  close(): void {
    this.closed = true;
    for (const c of this.watchAbortControllers) {
      try { c.abort(); } catch { /* ignore */ }
    }
  }

  private static defaultKubeConfig(): KubeConfig {
    const kc = new KubeConfig();
    try { kc.loadFromCluster(); } catch { kc.loadFromDefault(); }
    return kc;
  }
}
