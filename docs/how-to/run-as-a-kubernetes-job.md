# Run as a Kubernetes Job

The image has no TTY dependency and reads all credentials from environment/config. A fixed-purpose ingest Job can use workload identity or a Secret:

```yaml
apiVersion: batch/v1
kind: Job
metadata:
  name: tamsin-ingest
spec:
  backoffLimit: 2
  # Bound a wedged media tool or unreachable dependency at the Job level.
  activeDeadlineSeconds: 21600
  template:
    spec:
      restartPolicy: Never
      # Integrity cleanup has one 30-second deadline per affected batch. Leave enough time for
      # Kubernetes to deliver SIGTERM and for that cleanup to finish.
      terminationGracePeriodSeconds: 90
      securityContext:
        runAsNonRoot: true
        runAsUser: 65532
      containers:
        - name: tamsin
          image: ghcr.io/livewyer-ops/tamsin:1.0.0-rc.3 # pre-release; pin a digest
          securityContext:
            allowPrivilegeEscalation: false
            readOnlyRootFilesystem: true
            capabilities:
              drop: ["ALL"]
          args:
            - "--profile"
            - "essence-segments"
            - "-i"
            - "s3://incoming/day-001/"
            - "-o"
            - "https://tams.example.com"
            - "--format"
            - "json"
            - "--max-inputs"
            - "1000"
            - "--staging-byte-budget"
            - "80GiB"
          resources:
            requests:
              cpu: "2"
              memory: "2Gi"
              ephemeral-storage: "100Gi"
            limits:
              cpu: "8"
              memory: "8Gi"
              # Leave room above the emptyDir for the image, logs and runtime
              # writable layers, which also count as pod ephemeral storage.
              ephemeral-storage: "120Gi"
          envFrom:
            - secretRef:
                name: tamsin-auth
          volumeMounts:
            - name: temporary-media
              mountPath: /tmp
      volumes:
        - name: temporary-media
          emptyDir:
            sizeLimit: "100Gi"
```

The three numbers are intentionally different: TAMSin may reserve 80 GiB,
`emptyDir` permits 100 GiB, and the pod limit leaves another 20 GiB for its
writable layer and logs. Size them from the largest expected source plus its
generated essence/segment output, then account for `--concurrency`. The values
above are an operational example, not a universal media-profile recommendation.

`--format json` makes ingest stdout a live NDJSON event stream. Each container
log line is an independently valid `tamsin.ingest.events` record, including
progress, terminal Object/Flow/input records, and the final `run.finished`.
There is no final batch document for a log collector to reconstruct. Require a
`run.finished` record whose `payload.exit_code` matches the container status;
EOF without it means the run was forced down or its output channel failed.

An application which forks TAMSin instead of relying on pod logs must drain
stdout and stderr concurrently. stderr contains sanitised operator diagnostics
but is not part of the result protocol. Send SIGTERM for graceful cancellation
and leave enough time for registration/retraction cleanup; stdin is not a
control channel because it may carry media.

Configure the log collector to retain stdout when the NDJSON event stream is
the operational record. A hard kill can truncate that stream; absence of
`run.finished` must therefore remain distinguishable from success.

## See also

- [Authenticate against a TAMS store](authenticate.md)
- [Configuration reference](../reference/configuration.md)
