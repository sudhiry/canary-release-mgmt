import { CoreV1Api, KubeConfig, type V1Pod, Watch } from "@kubernetes/client-node";

export class XCanaryPresenceWatcher {
  private readonly podReady = new Map<string, boolean>();
  private canaryReady = false;
  private watchAbortController: AbortController | null = null;
  private closed = false;

  constructor(
    private readonly namespace: string,
    private readonly serviceName: string,
    private readonly kc: KubeConfig = defaultInClusterKubeConfig(),
  ) {}

  isCanaryReady(): boolean {
    return this.canaryReady;
  }

  async start(): Promise<void> {
    const labelSelector = `app=${this.serviceName},version=canary`;
    const coreApi = this.kc.makeApiClient(CoreV1Api);

    const list = await coreApi.listNamespacedPod(
      this.namespace,
      undefined, // pretty
      undefined, // allowWatchBookmarks
      undefined, // _continue
      undefined, // fieldSelector
      labelSelector,
    );
    for (const p of list.items) {
      const name = p.metadata?.name;
      if (name) this.podReady.set(name, isPodReady(p));
    }
    this.recomputeFlag();

    const watch = new Watch(this.kc);
    this.watchAbortController = await watch.watch(
      `/api/v1/namespaces/${this.namespace}/pods`,
      { labelSelector },
      (type: string, obj: V1Pod) => {
        const name = obj.metadata?.name;
        if (!name) return;
        if (type === "DELETED") {
          this.podReady.delete(name);
        } else {
          this.podReady.set(name, isPodReady(obj));
        }
        this.recomputeFlag();
      },
      (_err) => {
        if (!this.closed) {
          setTimeout(() => { void this.start(); }, 1000);
        }
      },
    );
  }

  close(): void {
    this.closed = true;
    if (this.watchAbortController) {
      try { this.watchAbortController.abort(); } catch { /* ignore */ }
    }
  }

  private recomputeFlag(): void {
    let any = false;
    for (const ready of this.podReady.values()) {
      if (ready) { any = true; break; }
    }
    this.canaryReady = any;
  }
}

function defaultInClusterKubeConfig(): KubeConfig {
  const kc = new KubeConfig();
  kc.loadFromCluster();
  return kc;
}

export function computeCanaryReady(pods: V1Pod[]): boolean {
  return pods.some(isPodReady);
}

export function isPodReady(pod: V1Pod): boolean {
  if (!pod.status?.conditions) return false;
  return pod.status.conditions.some((c) => c.type === "Ready" && c.status === "True");
}
