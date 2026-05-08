import type { AxiosInstance, InternalAxiosRequestConfig } from "axios";
import { isCanary } from "./x-canary-context.js";
import { X_CANARY_HEADER, X_CANARY_TRUE } from "./x-canary-constants.js";

export function attachXCanaryAxiosInterceptor(instance: AxiosInstance): void {
  instance.interceptors.request.use((config: InternalAxiosRequestConfig) => {
    if (isCanary() && !config.headers[X_CANARY_HEADER]) {
      config.headers[X_CANARY_HEADER] = X_CANARY_TRUE;
    }
    return config;
  });
}
