# Kubernetes — Expert Revision Playbook (Production, Interviews, Java/Spring)

> **Revision guide** — scan sections before interviews or on-call. Deep production depth with plain language. Targets lead/architect/DevOps roles in Java/Spring shops.

A comprehensive end-to-end reference covering Kubernetes architecture, workloads, networking, storage, scheduling, security, observability, production debugging, Spring Boot on K8s, GitOps, and 40+ lead-level interview Q&As. Sourced from Kubernetes official docs, CNCF guides, production war stories, r/kubernetes themes, and cloud provider runbooks.

**Related playbooks in this repo:**

- [Metrics & Observability Playbook](metrics-observability-playbook.md) — Micrometer, Prometheus, RED/USE, SLOs for apps running on K8s
- [Circuit Breaker Expert Playbook](circuit-breaker-expert-playbook.md) — fail-fast and graceful degradation when pods or dependencies fail
- [Bulkhead Expert Playbook](bulkhead-expert-playbook.md) — isolate thread pools and connection pools per dependency on K8s
- [Saga Expert Playbook](saga-expert-playbook.md) — async orchestration patterns for microservices deployed as Deployments
- [Kafka Expert Playbook](kafka-expert-playbook.md) — event-driven workloads on K8s (StatefulSet, operators)

---

## Table of Contents

1. [Kubernetes Architecture — Control Plane and Workers](#section-1-kubernetes-architecture--control-plane-and-workers)
2. [Pods, Containers, Init/Ephemeral, QoS Classes](#section-2-pods-containers-init-ephemeral-qos-classes)
3. [Workloads — Deployment, StatefulSet, DaemonSet, Job/CronJob](#section-3-workloads--deployment-statefulset-daemonset-jobcronjob)
4. [Networking — Services, Ingress, Gateway API, DNS, CNI](#section-4-networking--services-ingress-gateway-api-dns-cni)
5. [Configuration — ConfigMap, Secret, Downward API](#section-5-configuration--configmap-secret-downward-api)
6. [Storage — PV, PVC, StorageClass, CSI, StatefulSet Data](#section-6-storage--pv-pvc-storageclass-csi-statefulset-data)
7. [Scheduling — Requests/Limits, Affinity, Taints, Topology Spread, PDB](#section-7-scheduling--requestslimits-affinity-taints-topology-spread-pdb)
8. [Health Probes — Liveness, Readiness, Startup](#section-8-health-probes--liveness-readiness-startup)
9. [Deployments and Rollout Strategies](#section-9-deployments-and-rollout-strategies)
10. [Autoscaling — HPA, VPA, Cluster Autoscaler, KEDA](#section-10-autoscaling--hpa-vpa-cluster-autoscaler-keda)
11. [Security — RBAC, ServiceAccount, NetworkPolicy, Pod Security Standards](#section-11-security--rbac-serviceaccount-networkpolicy-pod-security-standards)
12. [Observability — Metrics, Logs, Events, kubectl Debugging](#section-12-observability--metrics-logs-events-kubectl-debugging)
13. [Five-Layer Production Debugging Framework](#section-13-five-layer-production-debugging-framework)
14. [Production Scenario Runbook — 25+ Scenarios](#section-14-production-scenario-runbook--25-scenarios)
15. [Spring Boot and Java on Kubernetes](#section-15-spring-boot-and-java-on-kubernetes)
16. [Helm, GitOps (Argo CD), Upgrades and Deprecated APIs](#section-16-helm-gitops-argo-cd-upgrades-and-deprecated-apis)
17. [Multi-Tenancy — Namespaces, Quotas, Limits](#section-17-multi-tenancy--namespaces-quotas-limits)
18. [Revision Cheat Sheets — kubectl Commands and Quick Tables](#section-18-revision-cheat-sheets--kubectl-commands-and-quick-tables)
19. [Lead Interview Questions — Logical and Production Scenarios](#section-19-lead-interview-questions--logical-and-production-scenarios)
20. [How to Talk About Kubernetes in an Interview](#section-20-how-to-talk-about-kubernetes-in-an-interview)
21. [Appendix — Decision Trees and API Quick Reference](#section-21-appendix--decision-trees-and-api-quick-reference)

---

## Section 1: Kubernetes Architecture — Control Plane and Workers

### 1.1 What Kubernetes Actually Does

Kubernetes is a **container orchestrator**. You tell it what you want (desired state): "run 3 replicas of this app, always healthy, with this config and storage." Kubernetes continuously reconciles actual state toward desired state.

It does **not** replace your app logic, CI/CD entirely, or observability stack — it is the **runtime platform** that schedules containers, routes traffic, mounts storage, and recovers from failures.

```
                    ┌─────────────────────────────────────┐
                    │           CONTROL PLANE             │
                    │  API Server │ etcd │ Scheduler      │
                    │  Controller Manager                 │
                    └──────────────┬──────────────────────┘
                                   │ REST / watch
         ┌─────────────────────────┼─────────────────────────┐
         │                         │                         │
    ┌────▼────┐               ┌────▼────┐               ┌────▼────┐
    │ Node 1  │               │ Node 2  │               │ Node 3  │
    │ kubelet │               │ kubelet │               │ kubelet │
    │ kube-   │               │ kube-   │               │ kube-   │
    │ proxy   │               │ proxy   │               │ proxy   │
    │ Pods    │               │ Pods    │               │ Pods    │
    └─────────┘               └─────────┘               └─────────┘
```

### 1.2 Control Plane Components

| Component | Role | Production notes |
|-----------|------|------------------|
| **kube-apiserver** | Front door to cluster; validates and persists all API objects | HA: 3+ instances behind LB; rate limits matter at scale; audit logs enabled |
| **etcd** | Distributed key-value store; cluster's source of truth | SSD only; quorum loss = cluster read-only or down; backup every hour minimum |
| **kube-scheduler** | Assigns Pods to Nodes based on resources, affinity, taints | Custom schedulers for GPU/special hardware; scheduler profiles in large shops |
| **kube-controller-manager** | Runs controllers (Deployment, ReplicaSet, Node, EndpointSlice, etc.) | Leader election; one active instance per controller type in HA setup |
| **cloud-controller-manager** | Integrates with cloud (LB, routes, node lifecycle) | Only on managed/cloud clusters or self-managed with cloud integration |

### 1.3 Worker Node Components

| Component | Role | Production notes |
|-----------|------|------------------|
| **kubelet** | Registers node; watches Pod specs; runs containers via CRI (containerd/CRI-O) | OOM/eviction decisions; cgroup driver must match runtime (systemd/cgroupfs) |
| **kube-proxy** | Implements Service networking (iptables/IPVS/eBPF) | IPVS mode at scale; eBPF via Cilium can replace kube-proxy |
| **Container runtime** | Actually runs containers (containerd, CRI-O) | Not Docker daemon directly since K8s 1.24+ (dockershim removed) |
| **CNI plugin** | Pod network (Calico, Cilium, Flannel, AWS VPC CNI) | Choice affects NetworkPolicy, encryption, observability |

### 1.4 How a Pod Gets Scheduled (End-to-End)

```
1. kubectl apply / CI deploy → API server validates → writes to etcd
2. Deployment controller sees new ReplicaSet → creates Pod objects
3. Scheduler filters nodes (resources, affinity, taints) → scores → binds Pod to Node
4. kubelet on that Node sees bound Pod → pulls images → starts containers
5. CNI assigns Pod IP → kube-proxy updates Service endpoints
6. Readiness probe passes → EndpointSlice updated → Ingress/Service routes traffic
```

**Interview one-liner:** "The API server is the only component that talks to etcd. Everything else watches the API server."

### 1.5 Managed vs Self-Managed

| Aspect | EKS/GKE/AKS | Self-managed (kubeadm, RKE) |
|--------|-------------|------------------------------|
| Control plane | Provider manages | You manage HA, upgrades, etcd backups |
| Node pools | Easy autoscaling integration | You wire Cluster Autoscaler + cloud IAM |
| Networking | Cloud CNI defaults | You choose and operate CNI |
| Cost | Control plane fee + nodes | Infra + operational burden |
| Best for | Most production Java shops | Regulated, multi-cloud, edge, cost-at-scale |

### 1.6 etcd — The Cluster Brain

- Stores all cluster state: Pods, Secrets, ConfigMaps, RBAC, etc.
- **Quorum:** 3-node etcd = tolerate 1 failure; 5-node = tolerate 2.
- **Slow etcd** manifests as: API latency spikes, `kubectl` hangs, scheduling delays, leader election flapping.
- **Backup:** `etcdctl snapshot save`; test restore quarterly.
- **Never** run application workloads on etcd nodes.

### 1.7 API Server Request Path

```
Client (kubectl, controller, kubelet)
    → Authentication (cert, token, OIDC)
    → Authorization (RBAC, webhook)
    → Admission (Mutating → Validating webhooks, Pod Security)
    → etcd persist
    → watch notifications to subscribers
```

Admission webhooks are where policies like OPA/Gatekeeper, Kyverno, and image scanning inject defaults or reject non-compliant manifests.

---

## Section 2: Pods, Containers, Init/Ephemeral, QoS Classes

### 2.1 Pod — The Atomic Unit

A **Pod** is one or more containers that share:

- Network namespace (one IP per Pod)
- IPC namespace
- Optional PID namespace (with `shareProcessNamespace: true`)
- Volumes mounted at Pod level

**You deploy Pods indirectly** via Deployments, StatefulSets, etc. — rarely create bare Pods in production.

### 2.2 Container Spec Essentials

```yaml
containers:
  - name: app
    image: myapp:1.2.3          # pin tag, never :latest in prod
    imagePullPolicy: IfNotPresent
    ports:
      - containerPort: 8080     # documentation; does not expose externally
    env:
      - name: SPRING_PROFILES_ACTIVE
        value: production
    resources:
      requests:
        memory: "512Mi"
        cpu: "250m"
      limits:
        memory: "768Mi"
        cpu: "1000m"
    securityContext:
      runAsNonRoot: true
      readOnlyRootFilesystem: true
      allowPrivilegeEscalation: false
```

### 2.3 Init Containers

Run **sequentially** before app containers start. Use for:

- Wait for dependency (DB migration sidecar pattern — prefer Job)
- Download config from vault-like init
- Set permissions on volumes
- Register with service mesh

```yaml
initContainers:
  - name: wait-for-db
    image: busybox:1.36
    command: ['sh', '-c', 'until nc -z postgres 5432; do sleep 2; done']
```

**Production tip:** Init container failure blocks Pod forever (CrashLoopBackOff on init). Set reasonable `activeDeadlineSeconds` on Job-style init or use startup probes instead of infinite wait loops.

### 2.4 Ephemeral Containers

Debug containers attached to a running Pod **without** restarting it. Cannot be declared in Pod spec ahead of time.

```bash
kubectl debug -it mypod --image=busybox --target=app -- sh
kubectl debug mypod --copy-to=mypod-debug --container=app -- sleep 3600
```

Use when: JVM heap dump needed, missing tools in production image, network debugging. **Not** a substitute for proper observability — see [metrics-observability-playbook.md](metrics-observability-playbook.md).

### 2.5 Sidecar Containers (Native Sidecars — K8s 1.28+)

Sidecars that start before app containers and run for Pod lifetime. Classic pattern: service mesh proxy, log shipper, config sync.

Legacy pattern: regular container in same Pod — all containers start in parallel unless init containers used.

### 2.6 QoS Classes

Kubernetes assigns QoS based on `resources.requests` and `resources.limits`:

| QoS Class | Condition | Eviction order |
|-----------|-----------|----------------|
| **Guaranteed** | Every container: limits == requests (for cpu and memory) | Last evicted |
| **Burstable** | At least one container has requests/limits set but not all Guaranteed | Middle |
| **BestEffort** | No requests or limits | First evicted |

**Java/Spring production rule:** Always set memory **requests** close to steady-state heap + metaspace + direct buffers. Set memory **limit** equal to request for Guaranteed QoS on critical payment/order services — or accept Burstable with careful sizing.

**CPU:** CPU is compressible — throttled when over limit. Memory is not — exceeding limit = **OOMKilled**.

### 2.7 Pod Lifecycle States

```
Pending → (scheduling, image pull) 
Running → (containers running)
Succeeded / Failed → (terminal)
Unknown → (node communication lost)
```

**Conditions:** `PodScheduled`, `Initialized`, `ContainersReady`, `Ready` (aggregate).

### 2.8 Restart Policy

| Policy | Use |
|--------|-----|
| `Always` | Deployments, default for long-running |
| `OnFailure` | Jobs |
| `Never` | Debugging, some CI workloads |

`restartPolicy` applies to **all containers** in Pod — not per-container.

### 2.9 terminationGracePeriodSeconds

Default 30 seconds. On Pod delete:

1. SIGTERM to containers
2. Wait grace period
3. SIGKILL if still running

**Spring Boot:** Must align `server.shutdown=graceful` + `spring.lifecycle.timeout-per-shutdown-phase` with this value. See Section 15.

---

## Section 3: Workloads — Deployment, StatefulSet, DaemonSet, Job/CronJob

### 3.1 Decision Matrix — When to Use Which

| Workload | Stable identity? | Storage per pod? | Scale pattern | Typical use |
|----------|------------------|------------------|---------------|-------------|
| **Deployment** | No | Usually shared/external DB | Horizontal, random scheduling | Spring Boot APIs, workers |
| **StatefulSet** | Yes (ordinal hostname) | PVC per pod | Ordered scale, sticky identity | Kafka, ZooKeeper, DBs, etcd |
| **DaemonSet** | Per-node | Optional | One (or N) per node | node-exporter, Fluent Bit, CNI |
| **Job** | No | Optional | Run-to-completion | Migrations, batch, one-off |
| **CronJob** | No | Optional | Scheduled | Reports, cleanup, cron backups |

### 3.2 Deployment

- Manages ReplicaSets; rolling updates by default.
- Pods are interchangeable — no stable network identity.
- `revisionHistoryLimit` controls old ReplicaSets kept for rollback.

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: order-service
spec:
  replicas: 3
  strategy:
    type: RollingUpdate
    rollingUpdate:
      maxSurge: 1
      maxUnavailable: 0
  selector:
    matchLabels:
      app: order-service
  template:
    metadata:
      labels:
        app: order-service
    spec:
      containers:
        - name: app
          image: order-service:2.1.0
```

**99% of Java microservices** should be Deployments behind a Service.

### 3.3 StatefulSet

- Stable Pod names: `web-0`, `web-1`, `web-2`
- Headless Service required for stable DNS: `web-0.web.default.svc.cluster.local`
- Ordered rollout and scale (0 → 1 → 2)
- `volumeClaimTemplates` for per-pod PVCs

Use when: application requires stable identity or local persistent storage (Kafka brokers, Redis cluster with local disk, Elasticsearch).

**Do not** use StatefulSet for stateless Spring Boot just because it "sounds important."

### 3.4 DaemonSet

Ensures a Pod runs on every (or selected) node.

Examples:

- `metrics-server`, `node-exporter`, `kube-proxy`
- Log collectors (Fluent Bit → Loki)
- Security agents

`tolerations` on DaemonSet allow running on tainted control-plane or GPU nodes.

### 3.5 Job and CronJob

**Job:** `completions`, `parallelism`, `backoffLimit`, `activeDeadlineSeconds`, `ttlSecondsAfterFinished`.

```yaml
apiVersion: batch/v1
kind: CronJob
metadata:
  name: nightly-report
spec:
  schedule: "0 2 * * *"
  concurrencyPolicy: Forbid
  successfulJobsHistoryLimit: 3
  jobTemplate:
    spec:
      backoffLimit: 2
      activeDeadlineSeconds: 3600
      template:
        spec:
          restartPolicy: OnFailure
          containers:
            - name: report
              image: report-job:1.0
```

**Spring Batch on K8s:** Run as Job triggered by CronJob or Argo Workflows — not as long-running Deployment.

### 3.6 ReplicaSet vs Deployment

ReplicaSet ensures N pods match selector. Deployment wraps ReplicaSet and adds rollout strategy. You always use Deployment in production — never manage ReplicaSets directly except for debugging rollouts.

---

## Section 4: Networking — Services, Ingress, Gateway API, DNS, CNI

### 4.1 Kubernetes Networking Model

Every Pod gets a routable IP (flat network). Pods can talk to any Pod without NAT (CNI responsibility). Services provide stable virtual IPs and DNS names.

### 4.2 Service Types

| Type | Behavior | Production use |
|------|----------|----------------|
| **ClusterIP** | Virtual IP, internal only | Default for microservices |
| **NodePort** | Opens port on every node | Dev, legacy; avoid in prod at scale |
| **LoadBalancer** | Cloud LB provisions | External entry when no Ingress |
| **ExternalName** | CNAME to external DNS | Point to SaaS API |
| **Headless** (`clusterIP: None`) | No cluster IP; DNS returns Pod IPs | StatefulSet peer discovery |

### 4.3 Service and Endpoints

```
Service (selector: app=order) → EndpointSlice (ready Pod IPs)
kube-proxy programs rules so ClusterIP traffic → Pod IPs
```

If readiness probe fails, Pod removed from Endpoints — **Service stops routing to it** even if process still runs.

### 4.4 Ingress

HTTP/HTTPS routing layer — host/path rules to Services.

```yaml
apiVersion: networking.k8s.io/v1
kind: Ingress
metadata:
  name: api-ingress
  annotations:
    nginx.ingress.kubernetes.io/ssl-redirect: "true"
spec:
  ingressClassName: nginx
  tls:
    - hosts: [api.example.com]
      secretName: api-tls
  rules:
    - host: api.example.com
      http:
        paths:
          - path: /orders
            pathType: Prefix
            backend:
              service:
                name: order-service
                port:
                  number: 80
```

**Ingress controllers:** nginx, Traefik, AWS ALB, GCE, HAProxy. One controller Deployment + IngressClass resource.

**Production issues:** wrong `ingressClassName`, backend Service port mismatch, Pod not Ready, cert-manager Certificate not issued, annotation typo.

### 4.5 Gateway API (Modern Replacement for Ingress)

Gateway API separates:

- **Gateway** — infra (LB, listeners, TLS)
- **HTTPRoute** — app routing rules
- **ReferenceGrant** — cross-namespace trust

Better for multi-team platforms: platform owns Gateway, teams own HTTPRoutes. Supported on GKE, EKS (via controllers), AKS.

### 4.6 CoreDNS

DNS names:

| Query | Resolves to |
|-------|-------------|
| `order-service` | Service in same namespace |
| `order-service.prod.svc.cluster.local` | FQDN |
| `10.96.0.1` | `kubernetes.default` API |
| Headless `web-0.web` | Pod IP directly |

**ndots:5** default — short names may need search path understanding. `dnsPolicy: ClusterFirst` is default.

### 4.7 CNI Basics

CNI plugin assigns Pod IPs and configures routes.

| CNI | NetworkPolicy | Notes |
|-----|---------------|-------|
| Calico | Yes | Popular, BGP or overlay |
| Cilium | Yes | eBPF, Hubble observability |
| Flannel | No (basic) | Simple overlay |
| AWS VPC CNI | Via Calico overlay addon | Pod IPs from VPC |

**NetworkPolicy** requires a CNI that enforces it — Flannel alone is insufficient.

### 4.8 kube-proxy Modes

- **iptables** — default, can be slow at thousands of Services
- **IPVS** — better at scale, supports more load balancing algorithms
- **Disabled** — Cilium/eBPF handles service load balancing

### 4.9 Common Networking Debug Commands

```bash
kubectl run tmp --rm -it --image=nicolaka/netshoot -- bash
# Inside: curl, dig, traceroute, tcpdump
dig order-service.default.svc.cluster.local
curl -v http://order-service:80/actuator/health
```

---

## Section 5: Configuration — ConfigMap, Secret, Downward API

### 5.1 ConfigMap

Non-sensitive configuration. Mounted as files or env vars.

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: app-config
data:
  application.yaml: |
    server:
      shutdown: graceful
    logging:
      level:
        root: INFO
```

**Hot reload:** ConfigMap update does **not** auto-reload app containers. Options: Reloader sidecar/operator, Spring Cloud Kubernetes refresh, restart via Deployment annotation bump.

### 5.2 Secret

Base64-encoded in etcd (not encrypted by default — enable encryption at rest).

Types: `Opaque`, `kubernetes.io/tls`, `kubernetes.io/dockerconfigjson`, `kubernetes.io/service-account-token`.

**Better production approach:** External Secrets Operator + AWS Secrets Manager / GCP Secret Manager / Vault. Sync into K8s Secret or mount directly.

```yaml
env:
  - name: DB_PASSWORD
    valueFrom:
      secretKeyRef:
        name: db-credentials
        key: password
```

**Never** commit Secrets to Git. Sealed Secrets or SOPS for GitOps encrypted secrets.

### 5.3 Downward API

Expose Pod/Container metadata into containers:

```yaml
env:
  - name: POD_NAME
    valueFrom:
      fieldRef:
        fieldPath: metadata.name
  - name: POD_IP
    valueFrom:
      fieldRef:
        fieldPath: status.podIP
  - name: CPU_REQUEST
    valueFrom:
      resourceFieldRef:
        containerName: app
        resource: requests.cpu
```

Use for: logging context, OpenTelemetry resource attributes, partition assignment hints.

### 5.4 Immutable ConfigMaps and Secrets (K8s 1.21+)

`immutable: true` — prevents accidental updates; forces new name/version for changes. Safer for GitOps — change = new resource name = rolling restart via Deployment.

### 5.5 Resource Quotas vs LimitRanges vs Pod resources

| Mechanism | Scope | Purpose |
|-----------|-------|---------|
| `resources` on Pod | Per container | Scheduling and cgroup limits |
| LimitRange | Namespace default/min/max | Default requests if omitted |
| ResourceQuota | Namespace aggregate | Cap total CPU/memory/Pod count |

---

## Section 6: Storage — PV, PVC, StorageClass, CSI, StatefulSet Data

### 6.1 Volume Types Overview

| Volume | Lifetime | Use |
|--------|----------|-----|
| emptyDir | Pod | Temp cache, scratch |
| configMap/secret | Pod | Config injection |
| persistentVolumeClaim | PVC | Durable data |
| hostPath | Node | Avoid in prod (node binding) |
| CSI volumes | PVC | Cloud disks, NFS, Ceph |

### 6.2 PV, PVC, StorageClass Flow

```
StorageClass (gp3, fast, standard)
    → PVC requests storage (size, access mode, class)
    → Provisioner creates PV dynamically
    → Pod mounts PVC
```

```yaml
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: data-pvc
spec:
  accessModes: [ReadWriteOnce]
  storageClassName: gp3
  resources:
  requests:
    storage: 100Gi
```

### 6.3 Access Modes

| Mode | Meaning |
|------|---------|
| RWO | Single node write |
| ROX | Many nodes read |
| RWX | Many nodes read-write (NFS, EFS, Filestore) |

**EBS/gp3 = RWO** — one Pod per volume typically. Scale-out stateful with StatefulSet + one PVC per Pod.

### 6.4 CSI (Container Storage Interface)

Modern standard for storage plugins. EBS, GCE PD, Azure Disk, Portworx, Rook/Ceph all via CSI drivers.

CSI snapshot API for backup: `VolumeSnapshot` → `VolumeSnapshotContent`.

### 6.5 StatefulSet + Data

```yaml
volumeClaimTemplates:
  - metadata:
      name: data
    spec:
      accessModes: [ReadWriteOnce]
      storageClassName: gp3
      resources:
        requests:
          storage: 50Gi
```

Pod `kafka-0` gets PVC `data-kafka-0`. **Deleting StatefulSet does not delete PVCs** by default — data survives. Deleting PVC deletes PV if reclaim policy `Delete`.

### 6.6 Reclaim Policies

| Policy | When PVC deleted |
|--------|------------------|
| Delete | PV and cloud disk deleted |
| Retain | PV kept; manual cleanup |

Production databases: often `Retain` + backup automation.

### 6.7 Java App Storage

Most Spring Boot services: **no local PVC** — state in PostgreSQL, Redis, Kafka. PVC only for: file uploads (prefer object storage S3/GCS), embedded Lucene (rare), batch export temp (emptyDir + S3 upload).

---

## Section 7: Scheduling — Requests/Limits, Affinity, Taints, Topology Spread, PDB

### 7.1 Requests vs Limits

| Resource | Request | Limit |
|----------|---------|-------|
| CPU | Used for scheduling; guaranteed minimum | Throttled if exceeded (compressible) |
| Memory | Used for scheduling | OOMKill if exceeded (hard) |

**Scheduler uses requests** to decide if node has capacity. **kubelet uses limits** for enforcement.

```yaml
resources:
  requests:
    cpu: "500m"      # 0.5 core
    memory: "1Gi"
  limits:
    cpu: "2000m"
    memory: "1Gi"    # equal = Guaranteed memory QoS
```

### 7.2 CPU Units

`1` = 1 vCPU/core. `500m` = 0.5 core. `100m` = 0.1 core.

Java containers: CPU request too low → scheduled on busy node → latency. CPU limit too low → throttling → GC pauses feel worse. Many Java shops set CPU limit **unset** (only request) to avoid throttling — accept Burstable QoS for CPU.

### 7.3 Node Affinity / Anti-Affinity

**Affinity** — prefer or require certain nodes.

```yaml
affinity:
  nodeAffinity:
    requiredDuringSchedulingIgnoredDuringExecution:
      nodeSelectorTerms:
        - matchExpressions:
            - key: workload
              operator: In
              values: [compute]
```

**Pod affinity/anti-affinity** — co-locate or spread Pods.

```yaml
podAntiAffinity:
  preferredDuringSchedulingIgnoredDuringExecution:
    - weight: 100
      podAffinityTerm:
        labelSelector:
          matchLabels:
            app: order-service
        topologyKey: kubernetes.io/hostname
```

Spread replicas across nodes for HA — avoid two order-service Pods on same node.

### 7.4 Taints and Tolerations

**Taint on node** — repels Pods unless they tolerate.

```bash
kubectl taint nodes node1 dedicated=gpu:NoSchedule
```

```yaml
tolerations:
  - key: dedicated
    operator: Equal
    value: gpu
    effect: NoSchedule
```

Common taints: `node.kubernetes.io/not-ready`, `node.kubernetes.io/unreachable` (automatic), custom `dedicated`, `spot=true:NoSchedule`.

**Taint + toleration** does not guarantee scheduling — still need resources and affinity.

### 7.5 Topology Spread Constraints

Spread Pods across zones/regions:

```yaml
topologySpreadConstraints:
  - maxSkew: 1
    topologyKey: topology.kubernetes.io/zone
    whenUnsatisfiable: DoNotSchedule
    labelSelector:
      matchLabels:
        app: order-service
```

Critical for multi-AZ HA — survive zone loss without losing majority of replicas.

### 7.6 Priority Classes

`PriorityClass` — high-priority Pods can preempt lower-priority Pods when nodes are full.

Use carefully: payment service preempts batch Jobs. Preemption causes evictions elsewhere.

### 7.7 PodDisruptionBudget (PDB)

Limits voluntary disruptions (drain, cluster upgrade):

```yaml
apiVersion: policy/v1
kind: PodDisruptionBudget
metadata:
  name: order-service-pdb
spec:
  minAvailable: 2
  selector:
    matchLabels:
      app: order-service
```

`minAvailable: 2` with 3 replicas → only 1 Pod can be disrupted at a time.

**Production mistake:** PDB `minAvailable` equal to replica count → **drain stuck forever**.

### 7.8 Overcommit and Eviction

When node memory pressure:

1. BestEffort Pods evicted first
2. Burstable Pods exceeding requests
3. Guaranteed last

`kubelet` eviction signals: `memory.available`, `nodefs.available`, `imagefs.available`.

---

## Section 8: Health Probes — Liveness, Readiness, Startup

### 8.1 The Three Probes

| Probe | Question it answers | Failure action | Traffic routed? |
|-------|---------------------|----------------|---------------|
| **Liveness** | Is the process deadlocked/corrupted? | kubelet **restarts** container | N/A |
| **Readiness** | Can this instance accept traffic now? | Removed from Service endpoints | No |
| **Startup** | Has slow-start app finished booting? | Blocks liveness until success | No until ready |

### 8.2 Spring Boot Actuator Endpoints

```yaml
livenessProbe:
  httpGet:
    path: /actuator/health/liveness
    port: 8080
  initialDelaySeconds: 0
  periodSeconds: 10
  failureThreshold: 3
readinessProbe:
  httpGet:
    path: /actuator/health/readiness
    port: 8080
  periodSeconds: 5
  failureThreshold: 3
startupProbe:
  httpGet:
    path: /actuator/health/readiness
    port: 8080
  failureThreshold: 30
  periodSeconds: 10
  # 30 * 10 = 300s max startup time before liveness kills it
```

`management.endpoint.health.probes.enabled=true` in Spring Boot 2.3+.

### 8.3 Production Mistakes

| Mistake | Symptom | Fix |
|---------|---------|-----|
| Liveness checks DB | DB down → all Pods restart → thundering herd | Readiness checks DB; liveness checks only JVM/thread health |
| Same endpoint for liveness and readiness | Transient slowness → restart instead of temporary unready | Separate probes |
| `initialDelaySeconds` too low on liveness | Restart loop during slow JVM start | Use startupProbe; set initialDelay 0 |
| HTTP probe hits heavy endpoint | Probe timeout → false unhealthy | Lightweight `/actuator/health/liveness` |
| Probe timeout < app response under load | Flapping Ready/NotReady | Increase `timeoutSeconds`; fix app |
| exec probe on JVM | Forks process each probe — CPU waste | Use httpGet |
| No startupProbe for 2min+ boot | Liveness kills during Flyway/Liquibase | startupProbe with high failureThreshold |

### 8.4 Probe Mechanics

- `periodSeconds` — how often to probe
- `timeoutSeconds` — probe must respond within
- `successThreshold` — consecutive successes to be healthy (default 1)
- `failureThreshold` — consecutive failures before action (default 3)

**Readiness failure** during rolling update: old Pods drain, new Pods must pass readiness before receiving traffic — enables zero-downtime if configured correctly (`maxUnavailable: 0`).

### 8.5 gRPC Health Probes (K8s 1.24+)

```yaml
livenessProbe:
  grpc:
    port: 9090
```

For non-HTTP Spring apps using gRPC — no grpc_health_probe sidecar needed.

---

## Section 9: Deployments and Rollout Strategies

### 9.1 Rolling Update (Default)

```yaml
strategy:
  type: RollingUpdate
  rollingUpdate:
    maxSurge: 25%        # extra Pods above desired during update
    maxUnavailable: 25%  # Pods below desired during update
```

**Zero-downtime recipe:**

- `replicas >= 2`
- `maxUnavailable: 0` or `maxSurge: 1`
- Readiness probe correctly configured
- `preStop` hook + graceful shutdown (Section 15)

### 9.2 Rollout Status and Control

```bash
kubectl rollout status deployment/order-service
kubectl rollout history deployment/order-service
kubectl rollout undo deployment/order-service
kubectl rollout undo deployment/order-service --to-revision=3
kubectl rollout pause deployment/order-service    # canary manual
kubectl rollout resume deployment/order-service
```

### 9.3 Blue/Green

Two full environments (or two Deployments + Service selector switch):

1. Deploy `order-service-green` with new version
2. Test green via internal Service or header routing
3. Switch production Service selector `version: green` (or Ingress backend)
4. Keep blue for instant rollback

Tools: Argo Rollouts, Flagger, manual Service patch.

**Cost:** 2x resources during switch window.

### 9.4 Canary

Gradually shift traffic to new version:

- **Ingress/nginx:** `canary` annotations weight
- **Argo Rollouts:** automated canary with metric analysis (Prometheus error rate)
- **Service mesh:** Istio/Linkerd traffic split

```yaml
# nginx ingress canary example
nginx.ingress.kubernetes.io/canary: "true"
nginx.ingress.kubernetes.io/canary-weight: "10"
```

Rollback: reduce weight to 0 or undo Deployment.

### 9.5 Recreate Strategy

`strategy.type: Recreate` — kill all old Pods before new ones. **Downtime.** Only for dev or single-replica Jobs.

### 9.6 Helm and GitOps Rollouts

Argo CD sync → Deployment image tag change → rolling update. Argo Rollouts CRD replaces Deployment for advanced strategies. See Section 16.

---

## Section 10: Autoscaling — HPA, VPA, Cluster Autoscaler, KEDA

### 10.1 Horizontal Pod Autoscaler (HPA)

Scales Pod count based on metrics:

```yaml
apiVersion: autoscaling/v2
kind: HorizontalPodAutoscaler
metadata:
  name: order-service-hpa
spec:
  scaleTargetRef:
    apiVersion: apps/v1
    kind: Deployment
    name: order-service
  minReplicas: 3
  maxReplicas: 20
  metrics:
    - type: Resource
      resource:
        name: cpu
        target:
          type: Utilization
          averageUtilization: 70
    - type: Pods
      pods:
        metric:
          name: http_server_requests_seconds_per_second
        target:
          type: AverageValue
          averageValue: "100"
  behavior:
    scaleDown:
      stabilizationWindowSeconds: 300
```

**Requires metrics-server** (or custom metrics adapter) for resource metrics. Custom metrics from Prometheus Adapter.

### 10.2 HPA Not Helping — Common Causes

| Cause | Fix |
|-------|-----|
| CPU request too low | HPA sees 100% at idle — fix requests |
| Missing metrics-server | Install metrics-server |
| `minReplicas` already at max | Raise max or fix app efficiency |
| Bottleneck is DB not CPU | Scale app does not help — scale DB or cache |
| Cooldown too aggressive | Tune `behavior.scaleDown` |
| Java CPU lazy | JVM warms up — use custom latency metric from Micrometer |

See [metrics-observability-playbook.md](metrics-observability-playbook.md) for RED metrics driving HPA.

### 10.3 Vertical Pod Autoscaler (VPA)

Adjusts **requests/limits** per container based on usage — not replica count.

Modes: Off (recommend only), Initial, Auto (recreate Pods on change).

**Conflict:** VPA + HPA on same CPU metric — use VPA for memory, HPA for custom metrics, or VPA `InPlace` (K8s 1.27+ alpha).

### 10.4 Cluster Autoscaler

Adds/removes **nodes** when Pods cannot schedule (Pending due to insufficient CPU/memory).

Requirements:

- Node groups with min/max size
- Pods must have requests set (autoscaler simulates scheduling)
- PDBs respected during scale-down
- `--scale-down-unneeded-time` default 10m

**Not magic:** If Pending due to affinity/taint — autoscaler cannot fix.

### 10.5 KEDA (Kubernetes Event-Driven Autoscaling)

Scales from 0 to N based on external events:

- Kafka lag (`kafka lag scaler`)
- RabbitMQ queue depth
- Prometheus query
- Cron
- AWS SQS, GCP Pub/Sub

```yaml
apiVersion: keda.sh/v1alpha1
kind: ScaledObject
metadata:
  name: order-consumer-scaler
spec:
  scaleTargetRef:
    name: order-consumer
  minReplicaCount: 0
  maxReplicaCount: 50
  triggers:
    - type: kafka
      metadata:
        bootstrapServers: kafka:9092
        consumerGroup: order-group
        topic: orders
        lagThreshold: "100"
```

Perfect for Kafka consumers in Java — see [kafka-expert-playbook.md](kafka-expert-playbook.md).

---

## Section 11: Security — RBAC, ServiceAccount, NetworkPolicy, Pod Security Standards

### 11.1 RBAC Model

| Object | Purpose |
|--------|---------|
| Role / ClusterRole | Set of permissions (verbs on resources) |
| RoleBinding / ClusterRoleBinding | Binds Role to User/Group/ServiceAccount |

```yaml
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  name: pod-reader
  namespace: prod
rules:
  - apiGroups: [""]
    resources: ["pods", "pods/log"]
    verbs: ["get", "list", "watch"]
```

**Principle of least privilege:** CI deploy SA gets `update deployments` in one namespace — not cluster-admin.

### 11.2 ServiceAccount

Identity for Pods talking to API server.

```yaml
spec:
  serviceAccountName: order-service
automountServiceAccountToken: false  # if app does not need K8s API
```

Workload Identity (GKE), IRSA (EKS) — bind SA to cloud IAM for S3/Secrets without static keys.

### 11.3 NetworkPolicy

Default K8s: all Pods can talk to all Pods (within network policy enforcement).

```yaml
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: order-service-netpol
spec:
  podSelector:
    matchLabels:
      app: order-service
  policyTypes: [Ingress, Egress]
  ingress:
    - from:
        - podSelector:
            matchLabels:
              app: ingress-nginx
      ports:
        - port: 8080
  egress:
    - to:
        - podSelector:
            matchLabels:
              app: postgres
      ports:
        - port: 5432
    - to:                          # DNS
        - namespaceSelector: {}
      ports:
        - port: 53
```

**Deny-by-default namespace:** apply `default-deny-all` then explicit allows.

### 11.4 Pod Security Standards (PSS)

Replaces PodSecurityPolicy (removed 1.25).

| Level | Description |
|-------|-------------|
| Privileged | Unrestricted |
| Baseline | No host namespaces, privileged containers |
| Restricted | Non-root, read-only rootfs, drop capabilities |

Enforce via namespace labels:

```yaml
pod-security.kubernetes.io/enforce: restricted
pod-security.kubernetes.io/audit: restricted
pod-security.kubernetes.io/warn: restricted
```

Spring Boot on distroless/non-root image fits Restricted with proper `securityContext`.

### 11.5 Secrets and Supply Chain

- Image scanning (Trivy, Grype) in CI
- Sign images (cosign), verify in admission
- No `cluster-admin` for developers in prod
- Audit logs for Secret access
- Encryption at rest for etcd

### 11.6 App-Level Resilience on K8s

When dependencies fail inside the cluster, combine:

- **Circuit breaker** — [circuit-breaker-expert-playbook.md](circuit-breaker-expert-playbook.md)
- **Bulkhead** — limit concurrent calls per downstream Service — [bulkhead-expert-playbook.md](bulkhead-expert-playbook.md)
- **Readiness** — stop accepting traffic when unhealthy
- **HPA** — scale when healthy but loaded

---

## Section 12: Observability — Metrics, Logs, Events, kubectl Debugging

### 12.1 The Three Pillars on K8s

| Pillar | K8s-native | Production stack |
|--------|------------|------------------|
| Metrics | metrics-server (CPU/mem) | Prometheus + Grafana + Micrometer |
| Logs | container stdout/stderr | Fluent Bit → Loki/ELK/CloudWatch |
| Traces | None native | OpenTelemetry → Tempo/Jaeger |

Full Micrometer/Prometheus depth: [metrics-observability-playbook.md](metrics-observability-playbook.md).

### 12.2 metrics-server

```bash
kubectl top nodes
kubectl top pods -n prod
kubectl top pods -n prod --containers
```

Cluster-level CPU/memory usage for HPA and human triage. Not a historical store — use Prometheus.

### 12.3 Prometheus on K8s

- **Prometheus Operator** — ServiceMonitor/PodMonitor scrape configs
- Scrape `/actuator/prometheus` on Spring Boot
- kube-state-metrics — Deployment replica health, Pod status
- node-exporter — node hardware metrics

Key alerts: Pod not Ready, deployment replicas mismatch, HPA at max, node NotReady, PVC almost full.

### 12.4 Logs

```bash
kubectl logs deployment/order-service --tail=100 -f
kubectl logs pod/order-service-abc -c app --previous   # crashed container
kubectl logs -l app=order-service --all-containers=true --tail=50
```

**Structured JSON logs** with `traceId`, `pod`, `namespace` — correlate with traces.

### 12.5 Events

```bash
kubectl get events -n prod --sort-by='.lastTimestamp'
kubectl get events --field-selector involvedObject.name=order-service-abc
```

Events explain: FailedScheduling, Unhealthy probe, Evicted, BackOff, FailedMount.

**Short retention** — export to monitoring or `eventrouter` for history.

### 12.6 Useful kubectl Debugging

```bash
kubectl describe pod <pod> -n prod          # events + spec + status
kubectl get pod <pod> -o yaml               # full state
kubectl exec -it <pod> -c app -- sh
kubectl port-forward svc/order-service 8080:80
kubectl debug node/<node> -it --image=ubuntu
```

### 12.7 RED Metrics for Services on K8s

From [metrics-observability-playbook.md](metrics-observability-playbook.md):

- **Rate** — requests/sec per Service
- **Errors** — 5xx rate
- **Duration** — P50/P99 latency

Map to HPA custom metrics and canary analysis.

---

## Section 13: Five-Layer Production Debugging Framework

When something breaks in production, **do not random kubectl**. Walk layers bottom-up or top-down systematically.

```
Layer 1: APPLICATION     — JVM, Spring, business logic, Actuator health
Layer 2: K8S WORKLOAD     — Pod status, probes, resources, events, logs
Layer 3: K8S NETWORKING   — Service, Endpoints, Ingress, DNS, NetworkPolicy
Layer 4: K8S CLUSTER      — Nodes, scheduler, etcd, control plane, CNI
Layer 5: INFRA / CLOUD    — LB, VPC, IAM, disk, region outage
```

### Layer 1: Application

**Symptoms:** 500 errors, slow responses, health check body shows DOWN.

```bash
kubectl logs -l app=order-service --tail=200
kubectl exec -it deploy/order-service -- curl -s localhost:8080/actuator/health | jq
kubectl exec -it deploy/order-service -- curl -s localhost:8080/actuator/metrics | head
```

Check: DB connection pool exhausted ([bulkhead-expert-playbook.md](bulkhead-expert-playbook.md)), circuit breaker OPEN ([circuit-breaker-expert-playbook.md](circuit-breaker-expert-playbook.md)), GC logs, thread dump.

### Layer 2: Kubernetes Workload

```bash
kubectl get pods -l app=order-service -o wide
kubectl describe pod <pod>
kubectl get events --field-selector involvedObject.name=<pod>
kubectl top pod <pod>
```

Check: CrashLoopBackOff, OOMKilled, probe failures, `Pending` scheduling, image pull errors, resource limits.

### Layer 3: Kubernetes Networking

```bash
kubectl get svc order-service -o wide
kubectl get endpointslices -l kubernetes.io/service-name=order-service
kubectl get ingress -o wide
kubectl describe ingress api-ingress
# From debug pod:
dig order-service.prod.svc.cluster.local
curl -v http://order-service.prod.svc.cluster.local:80/actuator/health
kubectl get networkpolicy -n prod
```

Check: zero endpoints (readiness), wrong port, Ingress class, TLS secret, NetworkPolicy blocking.

### Layer 4: Kubernetes Cluster

```bash
kubectl get nodes
kubectl describe node <node>
kubectl get componentstatuses    # deprecated but still informative
kubectl get --raw='/readyz?verbose'
kubectl get pods -n kube-system
```

Check: NotReady nodes, disk pressure, CNI pods failing, CoreDNS down, scheduler/controller errors in control plane logs.

### Layer 5: Infrastructure / Cloud

- Cloud console: LB target health, security groups, NACLs
- RDS/Cloud SQL connectivity from node
- IAM/IRSA for Secrets/S3
- Regional outage status page
- etcd latency metrics (managed control plane dashboards)

### Framework Decision Tree

```
User cannot reach API
├── curl from outside fails
│   ├── LB unhealthy → Layer 5 (target groups, certs)
│   └── LB healthy → Layer 3 (Ingress rules, backend Service)
├── curl from inside cluster fails
│   ├── Service has no endpoints → Layer 2 (readiness, pods)
│   └── endpoints exist → Layer 1 (app returns errors)
├── Pod CrashLoopBackOff → Layer 2 then Layer 1 (logs)
├── Pod Pending → Layer 2 (events) → Layer 4 (capacity) → Layer 5 (node pool)
```

### Time-Boxed On-Call Triage (First 5 Minutes)

1. `kubectl get pods -n <ns> | grep -v Running`
2. `kubectl get events -n <ns> --sort-by='.lastTimestamp' | tail -20`
3. Grafana: error rate, latency, saturation dashboards
4. Recent deploy? `kubectl rollout history`
5. Node health: `kubectl get nodes`

---

## Section 14: Production Scenario Runbook — 25+ Scenarios

### 14.1 CrashLoopBackOff

**Symptoms:** Pod restarts repeatedly; `CrashLoopBackOff` status.

**Diagnosis:**

```bash
kubectl describe pod <pod> | tail -30
kubectl logs <pod> --previous
kubectl logs <pod> -c <container>
```

| Root Cause | Fix |
|------------|-----|
| App exception on startup | Fix code/config; check logs |
| Liveness probe too aggressive | startupProbe; fix liveness path |
| Missing env/Secret | Add Secret; check `CreateContainerConfigError` |
| Wrong JVM heap vs memory limit | `-XX:MaxRAMPercentage` tuning (Section 15) |
| Flyway migration failure | Fix migration; Job for migrations instead |

### 14.2 OOMKilled

**Symptoms:** `Last State: Terminated, Reason: OOMKilled`; exit code 137.

**Diagnosis:**

```bash
kubectl describe pod <pod> | grep -A5 "Last State"
kubectl top pod <pod> --containers
```

| Root Cause | Fix |
|------------|-----|
| Heap larger than container memory limit | Set `MaxRAMPercentage`; limit = heap + non-heap headroom |
| Native memory leak (Netty, gRPC) | Increase limit; fix leak; direct memory limits |
| Memory spike under load | Increase limit; bulkhead limits concurrency |
| No memory limit set | Node OOM may kill random Pods — always set limits |

### 14.3 ImagePullBackOff / ErrImagePull

**Symptoms:** Pod `Pending` or `ImagePullBackOff`.

```bash
kubectl describe pod <pod> | grep -i image
```

| Root Cause | Fix |
|------------|-----|
| Wrong image tag | Correct image name/tag |
| Registry auth missing | `imagePullSecrets` or node IAM |
| Rate limited by registry | Retry; mirror registry; pull-through cache |
| Private registry DNS | Fix network/firewall |

### 14.4 Pod Pending — Insufficient CPU/Memory

```bash
kubectl describe pod <pod> | grep -A10 Events
```

Events: `0/5 nodes are available: 3 Insufficient memory, 2 Insufficient cpu`.

**Fix:** Add nodes (Cluster Autoscaler), reduce requests, delete unused workloads, fix over-provisioned requests.

### 14.5 Pod Pending — Affinity/Taints

Events: `didn't match Pod's node affinity`, `had taint`.

**Fix:** Adjust affinity rules, add tolerations, or label nodes correctly.

### 14.6 Pod Pending — PVC Not Bound

Events: `persistentvolumeclaim not found`, `volume binding failed`.

**Fix:** Create PVC, check StorageClass, quota, cloud disk limits.

### 14.7 CreateContainerConfigError

Missing Secret/ConfigMap key referenced in env.

```bash
kubectl describe pod <pod>
```

**Fix:** Create Secret or fix key name.

### 14.8 RunContainerError / StartError

Often volume mount permissions, seccomp, or binary not found (wrong architecture image).

### 14.9 Pod Evicted

Node pressure — disk or memory.

```bash
kubectl describe pod <pod> | grep Evicted
kubectl describe node <node> | grep -i pressure
```

**Fix:** Clean images (`imagefs`), add disk, reduce Pod density, fix BestEffort workloads consuming memory.

### 14.10 Node NotReady

```bash
kubectl describe node <node>
kubectl get pods -n kube-system -o wide | grep <node>
```

| Root Cause | Fix |
|------------|-----|
| kubelet stopped | Restart kubelet on node |
| CNI failure | Restart CNI pods; check networking |
| Disk pressure | Clean up node |
| Cloud instance issue | Replace node |

Pods on NotReady node: evicted after `pod-eviction-timeout` (default 5m) if not recovered.

### 14.11 Ingress Returns 502/503

```bash
kubectl describe ingress <ingress>
kubectl get endpointslices -l kubernetes.io/service-name=<backend-svc>
kubectl logs -n ingress-nginx deploy/ingress-nginx-controller --tail=50
```

| Root Cause | Fix |
|------------|-----|
| No ready endpoints | Fix readiness probe / app |
| Wrong Service port | Align `targetPort` and container port |
| Ingress class mismatch | Set `ingressClassName` |
| Upstream timeout | Increase proxy timeouts; fix app slowness |

### 14.12 Ingress TLS Certificate Issues

```bash
kubectl describe certificate -n prod
kubectl get certificaterequest,challenges -n prod
```

**Fix:** cert-manager issuer config, DNS01/HTTP01 challenge, expired cert renewal.

### 14.13 Service Works via Port-Forward but Not via Ingress

NetworkPolicy blocking ingress controller → app path. Ingress routing to wrong namespace Service.

### 14.14 DNS Resolution Fails Inside Pod

```bash
kubectl run -it --rm debug --image=busybox -- nslookup kubernetes.default
kubectl get pods -n kube-system -l k8s-app=kube-dns
```

**Fix:** CoreDNS pods, `dnsPolicy`, custom `dnsConfig`, NetworkPolicy blocking UDP 53.

### 14.15 HPA Not Scaling Up

```bash
kubectl describe hpa order-service-hpa
kubectl get --raw "/apis/metrics.k8s.io/v1beta1/namespaces/prod/pods" | jq
```

| Root Cause | Fix |
|------------|-----|
| metrics-server down | Restore metrics-server |
| CPU request too small | Fix resource requests |
| Already at maxReplicas | Increase max |
| Custom metric missing | Fix Prometheus adapter |

### 14.16 HPA Scaling Too Aggressively Down

**Fix:** `behavior.scaleDown.stabilizationWindowSeconds: 300`; ensure readiness during scale events.

### 14.17 Cluster Autoscaler Not Adding Nodes

```bash
kubectl logs -n kube-system deploy/cluster-autoscaler
```

Pending Pods must be unschedulable due to resources — not affinity. Check node group max size, IAM permissions.

### 14.18 Drain Stuck / PDB Blocking Eviction

```bash
kubectl drain <node> --ignore-daemonsets --delete-emptydir-data
```

Events: `Cannot evict pod: would violate PodDisruptionBudget`.

**Fix:** Temporarily adjust PDB (careful!), scale up replicas, `kubectl delete pod` with disruption allowed, or `--disable-eviction` (last resort).

### 14.19 etcd Slow / API Latency

**Symptoms:** `kubectl` slow; controllers lag; watch timeouts.

**Fix:** Check etcd metrics (disk latency, leader changes), defragment etcd (maintenance window), scale etcd disk IOPS, reduce object count (old ReplicaSets, events).

### 14.20 Deployment Rollout Stuck

```bash
kubectl rollout status deployment/<name>
kubectl get rs -l app=<app>
```

`ProgressDeadlineExceeded` — new Pods never become Ready.

**Fix:** readiness probe, image pull, resource limits, rollback `kubectl rollout undo`.

### 14.21 ConfigMap Updated but App Still Old Config

ConfigMap mount has ~60s kubelet sync delay; env var injection never updates without restart.

**Fix:** Reloader, restart rollout, or Spring Cloud Kubernetes refresh.

### 14.22 Secret Rotation Not Picked Up

Same as ConfigMap — restart Pods or use projected volumes with rotation (limited support).

### 14.23 NetworkPolicy — Sudden Connection Timeouts

After deploying default-deny NetworkPolicy.

**Fix:** Allow DNS egress (UDP/TCP 53), allow ingress controller namespace, allow dependency Services.

### 14.24 Java App Slow After K8s Migration

CPU throttling from low limits, noisy neighbor on Burstable QoS, missing CPU requests causing bad scheduling.

```bash
kubectl describe pod <pod> | grep -i throttl
# Check metrics: container_cpu_cfs_throttled_seconds_total
```

**Fix:** Increase CPU request; remove CPU limit for Java; use dedicated nodes.

### 14.25 Kafka Consumer Lag Growing on K8s

Consumers not scaling — check KEDA ScaledObject, HPA on wrong metric, consumer thread blocked, partition count < max replicas.

See [kafka-expert-playbook.md](kafka-expert-playbook.md).

### 14.26 Argo CD Sync Failed / OutOfSync Loop

```bash
argocd app get <app> --show-operation
kubectl get application -n argocd <app> -o yaml
```

**Fix:** ignore differences (replicas managed by HPA), server-side apply conflicts, CRD version skew, manual kubectl edit fighting GitOps.

### 14.27 Certificate Expired on Internal Service

Check `kubectl get secret <tls-secret> -o jsonpath='{.data.tls\.crt}' | base64 -d | openssl x509 -noout -dates`.

### 14.28 Multi-Attach Error for Volume

RWO volume still attached to another node — previous Pod on dead node.

**Fix:** Force detach volume in cloud console; delete stuck Pod; improve PDB and graceful shutdown.

### 14.29 API Server 429 Too Many Requests

Client QPS exceeded — CI pipeline, runaway controller.

**Fix:** API priority/fairness, reduce watch fanout, client backoff, etcd performance.

### 14.30 Control Plane Upgrade Failure

**Fix:** Follow provider runbook; verify etcd health before upgrade; roll one control plane node at a time; validate `kubectl get nodes` between steps.

---

## Section 15: Spring Boot and Java on Kubernetes

### 15.1 JVM Memory vs Container Limits

**The #1 Java-on-K8s production bug:** JVM thinks it has host memory; container has cgroup limit; JVM OOMKilled.

Before Java 10: `-Xmx` must be manually set below container memory limit.

**Java 8u191+, Java 11+:** Container-aware JVM reads cgroup limits.

```bash
JAVA_TOOL_OPTIONS="-XX:MaxRAMPercentage=75.0 -XX:InitialRAMPercentage=50.0"
```

| Container memory limit | Suggested MaxRAMPercentage | Heap ~ |
|------------------------|---------------------------|--------|
| 512Mi | 50-60% | 256-300Mi (tight — avoid) |
| 1Gi | 70-75% | ~700Mi |
| 2Gi | 75% | ~1.5Gi |

**Leave headroom for:** metaspace, thread stacks, direct buffers (Netty), native libs, GC overhead.

### 15.2 Memory Request Sizing

```
memory request ≈ steady-state heap + metaspace + direct memory + 20% buffer
memory limit   = request (Guaranteed) OR request + burst headroom (Burstable)
```

Monitor in production via Micrometer + Prometheus:

- `jvm.memory.used` heap/non-heap
- `container_memory_working_set_bytes`
- OOMKilled events

See [metrics-observability-playbook.md](metrics-observability-playbook.md) Section 15-16 for Actuator/Prometheus setup.

### 15.3 CPU for Java

- Set **CPU request** to expected sustained usage (profiling under load).
- **CPU limit** optional — many teams omit limit to avoid throttling during GC spikes.
- If limit set: watch `container_cpu_cfs_throttled_seconds_total`.

### 15.4 Graceful Shutdown

On Pod termination:

1. Pod removed from Service endpoints (if readiness fails on shutdown hook)
2. `preStop` hook runs
3. SIGTERM to JVM
4. `terminationGracePeriodSeconds` elapses
5. SIGKILL

```yaml
lifecycle:
  preStop:
    exec:
      command: ["sh", "-c", "sleep 5"]
spec:
  terminationGracePeriodSeconds: 60
```

```yaml
# application.yaml
server:
  shutdown: graceful
spring:
  lifecycle:
    timeout-per-shutdown-phase: 30s
```

**Why preStop sleep:** kube-proxy/Ingress endpoint propagation delay — traffic may still arrive during shutdown. 5s sleep is common; tune with your mesh/LB.

**Kafka consumers:** stop polling, commit offsets, then exit — Spring Kafka `shutdown-timeout`.

### 15.5 Actuator Health for K8s

```yaml
management:
  endpoint:
    health:
      probes:
        enabled: true
      show-details: when_authorized
  health:
    livenessstate:
      enabled: true
    readinessstate:
      enabled: true
```

**Liveness group:** exclude external dependencies.

```yaml
management.endpoint.health.group.liveness.include: livenessState, ping
management.endpoint.health.group.readiness.include: readinessState, db, redis
```

### 15.6 Spring Boot 3 + Native (GraalVM)

Smaller memory footprint, faster startup — good for K8s scale-to-zero with KEDA.

Trade-offs: reflection config, longer build, some libraries incompatible.

### 15.7 Resilience4j on K8s

When a downstream Pod is unhealthy (readiness failing), circuit breaker opens fast — see [circuit-breaker-expert-playbook.md](circuit-breaker-expert-playbook.md).

Bulkhead limits concurrent outbound calls — prevents one slow Service from exhausting your Pod's thread pool — [bulkhead-expert-playbook.md](bulkhead-expert-playbook.md).

```yaml
resilience4j:
  circuitbreaker:
  instances:
    inventoryService:
      failureRateThreshold: 50
      waitDurationInOpenState: 30s
  bulkhead:
  instances:
    inventoryService:
      maxConcurrentCalls: 25
```

### 15.8 12-Factor on K8s

| Factor | K8s mapping |
|--------|-------------|
| Config | ConfigMap, Secret, External Secrets |
| Backing services | Services, external cloud APIs |
| Port binding | `containerPort` + Service |
| Concurrency | HPA, bulkhead thread pools |
| Disposability | Graceful shutdown, fast startup |
| Logs | stdout → log collector |

### 15.9 Sample Production Deployment Snippet

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: order-service
spec:
  replicas: 3
  strategy:
    rollingUpdate:
      maxSurge: 1
      maxUnavailable: 0
  template:
    spec:
      serviceAccountName: order-service
      securityContext:
        runAsNonRoot: true
        fsGroup: 1000
      terminationGracePeriodSeconds: 60
      containers:
        - name: app
          image: registry.example.com/order-service:2.4.1
          ports:
            - containerPort: 8080
          env:
            - name: JAVA_TOOL_OPTIONS
              value: "-XX:MaxRAMPercentage=75.0 -XX:+UseG1GC"
          resources:
            requests:
              cpu: "500m"
              memory: "1Gi"
            limits:
              memory: "1Gi"
          startupProbe:
            httpGet:
              path: /actuator/health/readiness
              port: 8080
            failureThreshold: 30
            periodSeconds: 10
          readinessProbe:
            httpGet:
              path: /actuator/health/readiness
              port: 8080
            periodSeconds: 5
          livenessProbe:
            httpGet:
              path: /actuator/health/liveness
              port: 8080
            periodSeconds: 10
          lifecycle:
            preStop:
              exec:
                command: ["sh", "-c", "sleep 5"]
```

---

## Section 16: Helm, GitOps (Argo CD), Upgrades and Deprecated APIs

### 16.1 Helm

Package manager for K8s — chart = templated manifests + values.

```bash
helm upgrade --install order-service ./charts/order-service \
  -n prod -f values-prod.yaml \
  --set image.tag=2.4.1 \
  --wait --timeout 5m
```

**Production practices:**

- Pin chart version and app version
- Values per environment (`values-dev.yaml`, `values-prod.yaml`)
- `helm diff upgrade` before apply
- Hooks for DB migrations (prefer separate Job chart)
- Do not store secrets in values — use External Secrets

### 16.2 GitOps with Argo CD

```
Git repo (manifests/helm) → Argo CD syncs → cluster state
```

```yaml
apiVersion: argoproj.io/v1alpha1
kind: Application
metadata:
  name: order-service
  namespace: argocd
spec:
  project: prod
  source:
    repoURL: https://github.com/org/k8s-manifests
    path: apps/order-service
    targetRevision: main
    helm:
      valueFiles: [values-prod.yaml]
  destination:
    server: https://kubernetes.default.svc
    namespace: prod
  syncPolicy:
    automated:
      prune: true
      selfHeal: true
    syncOptions:
      - CreateNamespace=true
```

**Benefits:** auditable deploys, easy rollback (git revert), drift detection.

**selfHeal caution:** fights manual `kubectl scale` — use HPA or ignore differences for replicas.

### 16.3 Argo Rollouts (Canary/Blue-Green)

Replaces Deployment for advanced rollout — integrates Prometheus analysis.

### 16.4 Cluster Upgrades

**Order of operations:**

1. Upgrade control plane (managed provider or kubeadm)
2. Upgrade node kubelet/kube-proxy
3. Upgrade workloads (CRDs, operators first)
4. Validate deprecated API usage before upgrade

```bash
pluto detect-helm -o wide                    # deprecated APIs in manifests
kubectl get --raw /metrics | grep apiserver_requested_deprecated_apis
```

### 16.5 Deprecated APIs to Know

| Removed / Deprecated | Replacement |
|---------------------|-------------|
| `extensions/v1beta1 Ingress` | `networking.k8s.io/v1` |
| `policy/v1beta1 PodDisruptionBudget` | `policy/v1` |
| `batch/v1beta1 CronJob` | `batch/v1` |
| `autoscaling/v2beta2 HPA` | `autoscaling/v2` |
| `PodSecurityPolicy` | Pod Security Standards + admission |

**Always** pin `apiVersion` in manifests to stable GA versions.

### 16.6 CRD and Operator Upgrades

Upgrade order: CRD → operator controller → operands. Read operator release notes — Kafka Strimzi, Prometheus Operator breaking changes common.

---

## Section 17: Multi-Tenancy — Namespaces, Quotas, Limits

### 17.1 Namespace as Tenancy Boundary

| Pattern | Isolation level |
|---------|-----------------|
| Namespace per team/env | Soft — shared nodes, RBAC boundary |
| Namespace + NetworkPolicy | Network isolation between teams |
| Cluster per env (dev/staging/prod) | Stronger blast radius |
| Cluster per tenant (SaaS) | Strongest — expensive |

### 17.2 ResourceQuota

```yaml
apiVersion: v1
kind: ResourceQuota
metadata:
  name: team-a-quota
  namespace: team-a
spec:
  hard:
    requests.cpu: "20"
    requests.memory: 40Gi
    limits.cpu: "40"
    limits.memory: 80Gi
    pods: "50"
    persistentvolumeclaims: "10"
```

### 17.3 LimitRange

```yaml
apiVersion: v1
kind: LimitRange
metadata:
  name: default-limits
  namespace: team-a
spec:
  limits:
    - default:
        cpu: 500m
        memory: 512Mi
      defaultRequest:
        cpu: 100m
        memory: 256Mi
      type: Container
```

Prevents BestEffort Pods and caps max single-Pod consumption.

### 17.4 RBAC per Namespace

`RoleBinding` in namespace — developers `edit` in dev, `view` in prod, CI SA `deploy` in prod only.

### 17.5 Noisy Neighbor Prevention

- ResourceQuota at namespace level
- LimitRange max per Pod
- PriorityClasses — platform workloads higher than batch
- Dedicated node pools for heavy tenants (`nodeSelector` + taints)

---

## Section 18: Revision Cheat Sheets — kubectl Commands and Quick Tables

### 18.1 Essential kubectl

```bash
# Context and namespace
kubectl config get-contexts
kubectl config use-context prod-eks
kubectl config set-context --current --namespace=prod

# Get resources
kubectl get all -n prod
kubectl get pods -o wide -n prod
kubectl get deploy,rs,svc,ingress,pdb,hpa -n prod

# Describe and logs
kubectl describe pod <pod>
kubectl logs -f deploy/<name> --all-containers
kubectl logs <pod> --previous

# Exec and debug
kubectl exec -it deploy/<name> -- bash
kubectl port-forward svc/<name> 8080:80
kubectl debug -it <pod> --image=nicolaka/netshoot --target=app

# Apply and rollouts
kubectl apply -f manifest.yaml
kubectl diff -f manifest.yaml
kubectl rollout status deploy/<name>
kubectl rollout undo deploy/<name>

# Scale
kubectl scale deploy/<name> --replicas=5
kubectl autoscale deploy/<name> --min=3 --max=10 --cpu-percent=70

# Node operations
kubectl cordon <node>
kubectl drain <node> --ignore-daemonsets --delete-emptydir-data
kubectl uncordon <node>

# Top and metrics
kubectl top nodes
kubectl top pods -n prod

# Events
kubectl get events -A --sort-by='.lastTimestamp' | tail -30

# Force delete stuck pod
kubectl delete pod <pod> --grace-period=0 --force

# Output formats
kubectl get pod <pod> -o yaml
kubectl get pod <pod> -o jsonpath='{.status.containerStatuses[0].state}'
```

### 18.2 Quick Reference Tables

#### Workload Selection

| Need | Use |
|------|-----|
| Stateless API | Deployment |
| Stable network ID + disk | StatefulSet |
| Per-node agent | DaemonSet |
| One-shot task | Job |
| Scheduled task | CronJob |

#### Service Types

| Type | Reachable from |
|------|----------------|
| ClusterIP | Inside cluster |
| NodePort | Node IP:port |
| LoadBalancer | External LB |
| Headless | Pod IPs via DNS |

#### Probe Selection

| Probe | Use when |
|-------|----------|
| startupProbe | Boot > 30s |
| readinessProbe | Dependency-aware traffic gate |
| livenessProbe | Deadlock detection only |

#### QoS

| Class | When |
|-------|------|
| Guaranteed | limits == requests (all containers) |
| Burstable | Some resources set |
| BestEffort | Nothing set — avoid in prod |

#### Pod Status Troubleshooting

| Status | First check |
|--------|-------------|
| Pending | `describe` events — scheduling, PVC, image |
| CrashLoopBackOff | `logs --previous`, liveness probe |
| OOMKilled | memory limits vs JVM heap |
| ImagePullBackOff | image name, pull secrets |
| Evicted | node pressure |
| NotReady | readiness probe, app health |

#### kubectl Output Shortcuts

| Alias / flag | Meaning |
|--------------|---------|
| `-o wide` | Node, IP columns |
| `-o yaml` | Full manifest |
| `-w` | Watch |
| `--show-labels` | Show labels |
| `-l app=x` | Label selector |
| `--field-selector` | Field filter |

---

## Section 19: Lead Interview Questions — Logical and Production Scenarios

> Scenario-based questions inspired by production interview themes (Medium, r/kubernetes, Google/cloud hiring patterns). Answers are revision-length — expand with your own war stories.

### Fundamentals

**Q1: What is Kubernetes and what problem does it solve?**

**A:** K8s is a container orchestrator. It schedules Pods on nodes, keeps desired replica count, exposes Services, rolls out updates, and self-heals crashed containers. It solves "how do I run 500 Java services across 100 machines without manual SSH and scripts." *(Section 1)*

**Q2: Explain the control plane components.**

**A:** **API server** — front door for all requests. **etcd** — stores cluster state. **Scheduler** — picks nodes for new Pods. **Controller manager** — reconciliation loops (Deployment, ReplicaSet, etc.). **kubelet** on each node runs containers. **kube-proxy** — Service load balancing rules. *(Section 1)*

**Q3: What is a Pod vs a container?**

**A:** Pod is the smallest deployable unit — one or more containers sharing network namespace and volumes. Usually one app container per Pod. Sidecars share the Pod lifecycle. *(Section 2)*

**Q4: Deployment vs StatefulSet vs DaemonSet?**

**A:** **Deployment** — stateless, interchangeable Pods. **StatefulSet** — stable name, ordered rollout, persistent identity + PVCs (Kafka, DB). **DaemonSet** — one Pod per node (log agent, node exporter). *(Section 3)*

**Q5: What is etcd and why does it matter?**

**A:** Distributed key-value store holding all cluster state. Slow etcd → slow API server → everything feels broken. Backup etcd before upgrades; watch latency and db size. *(Sections 1, 14.29)*

### Networking

**Q6: How does a Service find Pods?**

**A:** **Label selector** on Service matches Pod labels. Endpoints (or EndpointSlice) list Pod IPs. kube-proxy or CNI dataplane routes traffic. If selector wrong → zero endpoints → connection refused. *(Section 4)*

**Q7: ClusterIP vs NodePort vs LoadBalancer?**

**A:** **ClusterIP** — internal only. **NodePort** — opens port on every node (dev/debug). **LoadBalancer** — cloud LB with external IP. Production external traffic: Ingress/Gateway → Service. *(Section 4)*

**Q8: Pod is Running but not reachable via Ingress — what do you check?**

**A:** End-to-end: Ingress rules/host/TLS → Service port/name → Endpoints not empty → readiness passing → NetworkPolicy allows path → app responds on probe path. `kubectl describe ingress`, `get endpointslices`, curl from debug pod. *(Sections 4, 14.1)*

**Q9: Explain CoreDNS resolution inside the cluster.**

**A:** Pods use `/etc/resolv.conf` pointing to kube-dns Service. Names like `my-svc.my-ns.svc.cluster.local`. **ndots:5** causes short names to try search domains first — can cause 5s delays; use FQDN or adjust ndots. *(Section 4)*

**Q10: What is a NetworkPolicy?**

**A:** Firewall for Pods — default allow all until you add policy. Default-deny requires explicit allow for DNS (53), ingress controller, dependencies. Common post-mortem: "we locked down the namespace and broke everything." *(Section 11)*

### Scheduling and Resources

**Q11: requests vs limits?**

**A:** **Request** — guaranteed minimum for scheduling. **Limit** — max allowed; exceed memory → OOMKill; exceed CPU → throttled. Set requests from production profiling; limits for memory usually match or exceed request for Java. *(Section 7)*

**Q12: Pod stuck Pending though nodes show free CPU — why?**

**A:** **Requests** not **capacity** — node may have CPU free but not enough unrequested CPU. Also: PVC unbound, node affinity, taints, ResourceQuota, pod anti-affinity, image pull secrets missing. `kubectl describe pod` events tell you. *(Section 14.3)*

**Q13: Explain QoS classes and eviction order.**

**A:** **Guaranteed** (limits=request) — last evicted. **Burstable** — middle. **BestEffort** (no requests) — first evicted under node pressure. Critical prod workloads should be Guaranteed or Burstable with proper requests. *(Section 2)*

**Q14: Critical pod evicted during node pressure — prevention?**

**A:** Set appropriate memory **requests** (Guaranteed QoS), use **PriorityClass** for critical apps, **PodDisruptionBudget**, avoid overcommitting nodes, monitor disk/memory pressure alerts. *(Sections 7, 14)*

**Q15: Taints and tolerations?**

**A:** **Taint** on node repels Pods unless Pod has matching **toleration**. Use for dedicated GPU nodes, spot instances, control-plane isolation. *(Section 7)*

### Probes and Lifecycle

**Q16: Liveness vs readiness vs startup?**

**A:** **Startup** — slow boot gate (disables others until success). **Readiness** — can this Pod receive traffic? Fails → removed from Service endpoints. **Liveness** — is process deadlocked? Fails → restart container. Never put DB checks on liveness. *(Section 8)*

**Q17: App works locally but CrashLoopBackOff on K8s?**

**A:** `kubectl logs --previous`. Common: wrong config/env, missing Secret, probe fails before app ready, OOM, read-only filesystem, wrong port. *(Section 14.2)*

**Q18: Rolling update caused downtime despite maxUnavailable: 0 — why?**

**A:** Readiness flapping, insufficient replicas, PDB blocking, preStop too short (connections dropped), Ingress still sending to terminating Pod, single replica. Fix: startupProbe, preStop sleep, minReadySeconds, canary/blue-green. *(Sections 9, 14)*

### Autoscaling and Performance

**Q19: HPA scales Pods but latency still high — why?**

**A:** HPA on CPU but bottleneck is DB, external API, lock contention, or too few Kafka partitions. Scale doesn't help if dependency saturated. Check RED metrics, thread pools, connection pools ([bulkhead playbook](bulkhead-expert-playbook.md)). *(Section 10)*

**Q20: How does HPA work?**

**A:** Reads metrics (usually CPU/memory via metrics-server, or custom via Prometheus adapter). Compares to target, adjusts Deployment replicas. Needs requests set on Pods; needs metrics available; cooldown periods prevent flapping. *(Section 10)*

**Q21: HPA vs VPA vs Cluster Autoscaler vs KEDA?**

**A:** **HPA** — more Pod replicas. **VPA** — right-size requests/limits (careful with Java). **Cluster Autoscaler** — more nodes when Pods pending. **KEDA** — scale on queue lag, Kafka, custom metrics including scale-to-zero. *(Section 10)*

### Storage and Stateful

**Q22: StatefulSet pod fails after node reboot — PVC bound — what breaks?**

**A:** RWO volume still attached to old node (Multi-Attach), pod scheduled elsewhere. Or app expects local data path. Check volume attachment in cloud console, pod events, ordered startup. *(Section 14.28)*

**Q23: PV vs PVC vs StorageClass?**

**A:** **PVC** — claim ("I need 100Gi"). **StorageClass** — provisioner template (dynamic PV). **PV** — actual volume. StatefulSet uses volumeClaimTemplates for per-pod PVCs. *(Section 6)*

### Security

**Q24: How does RBAC work?**

**A:** **Role/ClusterRole** defines permissions. **RoleBinding/ClusterRoleBinding** attaches to User/Group/ServiceAccount. Principle of least privilege — app SA gets only what it needs (often no API access). *(Section 11)*

**Q25: How do you enforce images from trusted registry only?**

**A:** Admission policy: **Kyverno**, **OPA Gatekeeper**, or cloud policy (EKS/GKE). Deny `image:` not matching `registry.example.com/*`. Scan in CI (Trivy). *(Section 16)*

**Q26: Pod Security Standards vs PodSecurityPolicy?**

**A:** PSP deprecated. **PSS** (restricted/baseline/privileged) enforced via built-in admission labels on namespace. Restricted: non-root, no privileged, drop capabilities. *(Section 11)*

### Debugging and Operations

**Q27: All pods restart without a new deployment — why?**

**A:** Node NotReady/cycle, kubelet restart, OOM on node, CNI flap, liveness probe too aggressive, memory limit too low cluster-wide, node drain, spot interruption. Check events and node conditions. *(Section 14)*

**Q28: Node NotReady but VM healthy — investigate?**

**A:** kubelet not running, CNI pod crash, disk pressure, cert expiry, network to control plane, PLEG issues. SSH/node logs, `journalctl -u kubelet`. *(Section 14.6)*

**Q29: Node drain stuck during upgrade — what blocks eviction?**

**A:** PDB too strict, Pod with local storage without deletion allowed, standalone Pod (no controller), unsafe-to-evict annotation missing on system pods. `kubectl drain` output lists blockers. *(Section 14.7)*

**Q30: Walk through your debugging approach for "502 from Ingress".**

**A:** Five-layer framework: (1) app health inside Pod, (2) Pod status/probes, (3) Service endpoints + Ingress backend + NetworkPolicy, (4) node/CNI/CoreDNS, (5) cloud LB target health. *(Section 13)*

**Q31: OOMKilled but Grafana memory looks fine — why?**

**A:** Grafana shows **working set** average; OOM uses **cgroup limit** peak. JVM heap + metaspace + direct memory spike above limit. Or wrong container in multi-container Pod. Check `lastState.terminated.reason`. *(Section 14.9)*

**Q32: etcd performance degrading — causes?**

**A:** Disk latency (must be SSD), large cluster object count, slow watches, defragmentation needed, anti-virus on disk, network split. Monitor etcd metrics: db size, fsync duration, leader changes. *(Section 14)*

### Java / Spring on K8s

**Q33: How do you size memory for Spring Boot on K8s?**

**A:** Container limit = heap (MaxRAMPercentage ~75%) + metaspace + direct buffers + headroom. Enable Actuator probes. Graceful shutdown + preStop. See Section 15.

**Q34: Why preStop sleep before SIGTERM?**

**A:** Endpoints removal and LB propagation lag — without sleep, traffic still hits Pod during shutdown. Pair with `server.shutdown=graceful`. *(Section 15.4)*

### GitOps and Upgrades

**Q35: Cluster upgrade breaks workloads — prevention?**

**A:** Run `pluto` or API deprecation metrics before upgrade. Pin GA apiVersions. Upgrade CRDs/operators first. Test in staging cluster same K8s version. *(Section 16.4)*

**Q36: GitOps vs kubectl apply in prod?**

**A:** GitOps (Argo CD) — Git is source of truth, auditable, rollback via revert. kubectl apply — fine for debug, causes drift. Use selfHeal carefully with HPA-managed replicas. *(Section 16)*

### Advanced Scenarios

**Q37: Zero-downtime deployment strategy for Java monolith on K8s?**

**A:** Rolling update maxUnavailable: 0, readiness on real traffic path, preStop + graceful shutdown, PDB minAvailable, optional canary via Argo Rollouts or separate Service weight split. *(Section 9)*

**Q38: How do you run Kafka on Kubernetes?**

**A:** StatefulSet + headless Service + PVCs per broker, careful with storage performance, operators (Strimzi). See [kafka-expert-playbook.md](kafka-expert-playbook.md). *(Section 3)*

**Q39: Ingress controller overloaded at peak — fix?**

**A:** HPA on ingress controller, increase replicas, check backend connection limits, enable keep-alive, shard ingress by host, move to cloud LB + Gateway API, rate limit at edge. *(Section 14)*

**Q40: Difference between Kubernetes and Docker Swarm / Nomad?**

**A:** K8s — richest ecosystem, steepest ops cost, default for cloud-native. Swarm — simpler, declining. Nomad — HashiCorp, multi-workload. Interview answer: pick K8s for team hiring pool and CNCF tooling; acknowledge complexity cost. *(Section 1)*

**Q41: What metrics do you alert on for a production cluster?**

**A:** Node NotReady, pod restart rate, OOMKilled count, pending pods > 0 sustained, API server latency, etcd fsync, ingress 5xx, HPA at max replicas, PVC almost full, cert expiry. *(Sections 12, 14)*

**Q42: How does Kubernetes relate to microservice patterns in this repo?**

**A:** Deployments run saga orchestrators and CQRS services; outbox relay runs as Deployment; circuit breaker/bulkhead in app code protect cross-Service calls; HPA scales read side; Strangler Fig uses Ingress weight split. Patterns live in app logic — K8s provides runtime and networking.

---

## Section 20: How to Talk About Kubernetes in an Interview

> Plain English. Use this as last-minute revision the night before.

---

### "What is Kubernetes in one sentence?"

It's a system that runs your containers across many machines — it decides which machine runs which container, restarts them if they crash, rolls out new versions, and gives them stable network names.

---

### "How is it different from Docker?"

Docker builds and runs one container on one machine. Kubernetes manages **many** containers on **many** machines — scheduling, scaling, networking, secrets, upgrades.

---

### "What's the first thing you check when a pod is broken?"

`kubectl describe pod` for events, then `kubectl logs --previous` if it restarted. That tells you scheduling problems, OOM, probe failures, or app stack traces — in that order before guessing.

---

### "What's the biggest mistake teams make with Java on K8s?"

Setting a 512Mi memory limit but letting the JVM default to thinking it has more heap than the container allows — instant OOMKilled. Fix with MaxRAMPercentage and Actuator readiness probes.

---

### Quick Answers (Revision Table)

| Question | Say this |
|---|---|
| Smallest unit? | Pod (usually one app container) |
| Control plane? | API server, etcd, scheduler, controllers |
| Service purpose? | Stable IP/DNS → Pod backends via labels |
| Ingress purpose? | HTTP routing + TLS from outside |
| Readiness vs liveness? | Readiness = traffic; liveness = restart if deadlocked |
| Pending pod? | describe pod — requests, PVC, taints, affinity |
| CrashLoopBackOff? | logs --previous, then probes, then config |
| OOMKilled? | memory limit vs JVM heap — raise or tune MaxRAMPercentage |
| HPA not helping? | Bottleneck isn't CPU — check DB, queues, dependencies |
| Zero downtime? | maxUnavailable 0, good readiness, preStop, graceful shutdown |
| Secure namespace? | RBAC + NetworkPolicy + PSS restricted + no secrets in Git |
| Debug 502? | Ingress → Service → Endpoints → Pod → app (5 layers) |
| GitOps? | Git is truth; Argo CD syncs; rollback = git revert |
| Stateful app? | StatefulSet + PVC + headless Service |

---

## Section 21: Appendix — Decision Trees and API Quick Reference

### 21.1 "Which workload do I use?"

```
Need stable pod name + disk?
  YES → StatefulSet
  NO → Need one per node?
         YES → DaemonSet
         NO → One-time/scheduled?
                YES → Job / CronJob
                NO → Deployment
```

### 21.2 "Which probe do I add?"

```
Boot time > 30s?
  YES → startupProbe first
Readiness checks dependencies?
  YES → readinessProbe on /actuator/health/readiness
Risk of deadlock without crash?
  YES → livenessProbe on /actuator/health/liveness (minimal checks)
```

### 21.3 Revision Day Checklist (Print This)

- [ ] Architecture: API server, etcd, scheduler, kubelet
- [ ] Service → Endpoints → Pod label flow
- [ ] requests vs limits vs QoS eviction
- [ ] Three probes and common mistakes
- [ ] Rolling update + PDB + preStop
- [ ] HPA requirements (requests, metrics)
- [ ] RBAC + NetworkPolicy basics
- [ ] 5-layer debug framework
- [ ] Java: MaxRAMPercentage, graceful shutdown, Actuator probes
- [ ] kubectl: describe, logs --previous, get events, rollout undo

### 21.4 Stable GA apiVersions (2024+)

| Resource | apiVersion |
|----------|------------|
| Deployment | apps/v1 |
| Service | v1 |
| Ingress | networking.k8s.io/v1 |
| HPA | autoscaling/v2 |
| PDB | policy/v1 |
| CronJob | batch/v1 |
| NetworkPolicy | networking.k8s.io/v1 |

### 21.5 Further Reading

| Topic | In this repo |
|-------|----------------|
| App metrics on K8s | [metrics-observability-playbook.md](metrics-observability-playbook.md) |
| Resilience in pods | [circuit-breaker-expert-playbook.md](circuit-breaker-expert-playbook.md), [bulkhead-expert-playbook.md](bulkhead-expert-playbook.md) |
| Event-driven on K8s | [kafka-expert-playbook.md](kafka-expert-playbook.md) |
| Migration routing | [strangler-fig-playbook.md](strangler-fig-playbook.md) |

---

*End of Kubernetes Expert Revision Playbook — re-read Section 18 cheat sheets and Section 19 Q&A before interviews or on-call handoff.*

