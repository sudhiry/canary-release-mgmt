import type { AxiosInstance, AxiosResponse } from "axios";
import { appendChain, X_SERVED_CHAIN_HEADER } from "./x-served-chain-context.js";

export function attachXServedChainAxiosInterceptor(axiosInstance: AxiosInstance): void {
  axiosInstance.interceptors.response.use((response: AxiosResponse) => {
    const headers = response.headers as Record<string, string | string[] | undefined>;
    const raw = headers[X_SERVED_CHAIN_HEADER];
    const value = Array.isArray(raw) ? raw.join(",") : raw;
    if (value) appendChain(value);
    return response;
  });
}
