{{- define "git-rs.fullname" -}}
{{- default (printf "%s-%s" .Release.Name (default .Chart.Name .Values.nameOverride)) .Values.fullnameOverride | trunc 48 | trimSuffix "-" -}}
{{- end -}}

{{- define "git-rs.selectorLabels" -}}
app.kubernetes.io/name: git-rs
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{- define "git-rs.labels" -}}
{{ include "git-rs.selectorLabels" . }}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end -}}

{{- define "git-rs.image" -}}
{{- if .Values.image.digest -}}
{{ printf "%s@%s" .Values.image.repository .Values.image.digest }}
{{- else -}}
{{ printf "%s:%s" .Values.image.repository (default .Chart.AppVersion .Values.image.tag) }}
{{- end -}}
{{- end -}}

{{- define "git-rs.serviceAccount" -}}
{{- if .Values.serviceAccount.create -}}
{{- default (include "git-rs.fullname" .) .Values.serviceAccount.name -}}
{{- else -}}
{{- default "default" .Values.serviceAccount.name -}}
{{- end -}}
{{- end -}}
{{- define "git-rs.pgName" -}}
{{- default (printf "%s-pg" (include "git-rs.fullname" .)) .Values.cnpg.name -}}
{{- end -}}
{{- define "git-rs.bucketClaim" -}}
{{- default (printf "%s-packs" (include "git-rs.fullname" .)) .Values.objectBucket.name -}}
{{- end -}}
{{- define "git-rs.pgPasswordSecret" -}}
{{- if .Values.cnpg.enabled -}}
{{ default (printf "%s-app" (include "git-rs.pgName" .)) .Values.postgresql.passwordSecret }}
{{- else -}}
{{ required "postgresql.passwordSecret is required for external PostgreSQL" .Values.postgresql.passwordSecret }}
{{- end -}}
{{- end -}}
{{- define "git-rs.pgCASecret" -}}
{{- if .Values.cnpg.enabled -}}
{{ default (printf "%s-ca" (include "git-rs.pgName" .)) .Values.postgresql.serverCASecret }}
{{- else -}}
{{ required "postgresql.serverCASecret is required for external PostgreSQL" .Values.postgresql.serverCASecret }}
{{- end -}}
{{- end -}}

{{- define "git-rs.pgEnv" -}}
- name: PGHOST
  {{- if .Values.cnpg.enabled }}
  value: {{ default (printf "%s-rw" (include "git-rs.pgName" .)) .Values.postgresql.host | quote }}
  {{- else }}
  value: {{ required "postgresql.host is required for external PostgreSQL" .Values.postgresql.host | quote }}
  {{- end }}
- name: PGPORT
  value: {{ .Values.postgresql.port | quote }}
- name: PGDATABASE
  value: {{ .Values.postgresql.database | quote }}
- name: PGUSER
  value: {{ .Values.postgresql.username | quote }}
- name: PGPASSWORD
  valueFrom:
    secretKeyRef:
      name: {{ include "git-rs.pgPasswordSecret" . }}
      key: {{ .Values.postgresql.passwordKey }}
- name: PGSSLMODE
  value: verify-full
- name: PGTARGETSESSIONATTRS
  value: read-write
- name: PGSSLROOTCERT
  value: /etc/git-rs/pg-ca/ca.crt
{{- end -}}

{{- define "git-rs.s3Env" -}}
- name: GIT_RS_S3_ENDPOINT
  value: {{ required "s3.endpoint is required (explicit HTTPS endpoint)" .Values.s3.endpoint | quote }}
- name: GIT_RS_S3_REGION
  value: {{ .Values.s3.region | quote }}
- name: GIT_RS_S3_BUCKET
  {{- if .Values.objectBucket.enabled }}
  valueFrom:
    configMapKeyRef:
      name: {{ include "git-rs.bucketClaim" . }}
      key: BUCKET_NAME
  {{- else }}
  value: {{ required "s3.bucket is required without a bucket claim" .Values.s3.bucket | quote }}
  {{- end }}
{{- $credentials := .Values.s3.credentialsSecret -}}
{{- if and (not $credentials) .Values.objectBucket.enabled -}}
{{- $credentials = include "git-rs.bucketClaim" . -}}
{{- end }}
- name: GIT_RS_S3_ACCESS_KEY
  valueFrom:
    secretKeyRef:
      name: {{ required "s3.credentialsSecret is required without a bucket claim" $credentials }}
      key: {{ .Values.s3.accessKeyKey }}
- name: GIT_RS_S3_SECRET_KEY
  valueFrom:
    secretKeyRef:
      name: {{ $credentials }}
      key: {{ .Values.s3.secretKeyKey }}
{{- if .Values.s3.caSecret }}
- name: SSL_CERT_FILE
  value: /etc/git-rs/s3-ca/ca.crt
{{- end }}
{{- end -}}
