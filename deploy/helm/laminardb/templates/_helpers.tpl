{{/*
Expand the name of the chart.
*/}}
{{- define "laminardb.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
*/}}
{{- define "laminardb.fullname" -}}
{{- if .Values.fullnameOverride }}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- $name := default .Chart.Name .Values.nameOverride }}
{{- if contains $name .Release.Name }}
{{- .Release.Name | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}
{{- end }}

{{/*
Create chart name and version as used by the chart label.
*/}}
{{- define "laminardb.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels.
*/}}
{{- define "laminardb.labels" -}}
helm.sh/chart: {{ include "laminardb.chart" . }}
{{ include "laminardb.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
Selector labels.
*/}}
{{- define "laminardb.selectorLabels" -}}
app.kubernetes.io/name: {{ include "laminardb.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
Service account name.
*/}}
{{- define "laminardb.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (include "laminardb.fullname" .) .Values.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.serviceAccount.name }}
{{- end }}
{{- end }}

{{/*
Config checksum annotation for rolling updates on config changes.
*/}}
{{- define "laminardb.configChecksum" -}}
checksum/config: {{ include (print $.Template.BasePath "/configmap.yaml") . | sha256sum }}
{{- end }}

{{/*
Determine if we should use StatefulSet (instead of Deployment).
Use StatefulSet when: persistence is enabled OR delta mode is active.
*/}}
{{- define "laminardb.useStatefulSet" -}}
{{- if or (and .Values.persistence.state.enabled (not .Values.persistence.state.useEmptyDir)) (and .Values.persistence.checkpoints.enabled) (eq .Values.laminardb.mode "delta") }}
true
{{- end }}
{{- end }}

{{/*
Image reference with tag.
*/}}
{{- define "laminardb.image" -}}
{{- $tag := default .Chart.AppVersion .Values.image.tag }}
{{- printf "%s:%s" .Values.image.repository $tag }}
{{- end }}

{{/*
Headless service name for delta mode discovery.
*/}}
{{- define "laminardb.headlessServiceName" -}}
{{- printf "%s-headless" (include "laminardb.fullname" .) }}
{{- end }}
