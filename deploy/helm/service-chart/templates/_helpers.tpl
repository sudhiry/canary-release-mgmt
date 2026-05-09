{{/*
Common labels applied to every resource managed by this chart.
*/}}
{{- define "service-chart.labels" -}}
app: {{ .Values.serviceName }}
version: {{ .Values.version | default "stable" }}
managed-by: helm
{{- end -}}

{{/*
Selector labels (subset of common labels) — used by Deployment.spec.selector
and matched by the headless Service via app:.
*/}}
{{- define "service-chart.selectorLabels" -}}
app: {{ .Values.serviceName }}
version: {{ .Values.version | default "stable" }}
{{- end -}}

{{/*
Resource name = serviceName + version (e.g., audit-service-stable, audit-service-canary).
Used for Deployment name; Service name is plain serviceName.
*/}}
{{- define "service-chart.resourceName" -}}
{{ .Values.serviceName }}-{{ .Values.version | default "stable" }}
{{- end -}}
