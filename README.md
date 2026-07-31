# Flink SQL Kurs - CP Flink Angepasste Version

**Original-Kurs:** https://developer.confluent.io/courses/flink-sql/  
**Instructor:** David Anderson, Principal Software Practice Lead  
**Angepasst für:** Confluent Platform 8.1.0 + CP Flink (On-Prem)

**Basis Demo Environment:** https://github.com/ethaden/confluent-platform-flink  
Eikes Confluent Platform Demo mit Apache Flink auf Kubernetes (KIND)

---

## 📋 Überblick

Dieser Kurs zeigt fortgeschrittene Apache Flink SQL Konzepte - angepasst für **Confluent Platform Flink (CP Flink)** auf Kubernetes. Alle Module verwenden dieselbe Struktur, nur die SQL-Syntax wurde für CP Flink angepasst.

**Was ist anders zur Cloud Demo (auf Confluent Developer)?**
- ✅ Syntax angepasst für CP Flink
- ✅ Kubernetes-native Setup
- ✅ Datagen Connectors für Test-Daten
- ✅ Alle Befehle copy-paste ready (hoffentlich)


---

## 🎓 Kursinhalte - Alle 15 Module

| # | Modul | Typ | Datei | Dauer |
|---|-------|-----|-------|-------|
| 1 | Apache Flink SQL Overview | 📖 Reading | [MODULE_01](MODULE_01_OVERVIEW.md) | 5 min |
| 2 | What is Flink SQL? Is it a database? | 📖 + 🎥 | [MODULE_02](MODULE_02_WHAT_IS_FLINK_SQL.md) | 15 min |
| 3 | **Get Started** | ✏️ Exercise | [MODULE_03](MODULE_03_EXERCISE_GET_STARTED.md) | 30 min |
| 4 | How Streaming SQL Uses Watermarks | 📖 + 🎥 | [MODULE_04](MODULE_04_WATERMARKS.md) | 10 min |
| 5 | **Hands-on with Watermarks** | ✏️ Exercise | [MODULE_05](MODULE_05_EXERCISE_WATERMARKS.md) | 30 min |
| 6 | Window Aggregations | 📖 + 🎥 | [MODULE_06](MODULE_06_WINDOW_AGGREGATIONS.md) | 6 min |
| 7 | OVER Aggregations | 📖 + 🎥 | [MODULE_07](MODULE_07_OVER_AGGREGATIONS.md) | 5 min |
| 8 | **Streaming Analytics** | ✏️ Exercise | [MODULE_08](MODULE_08_EXERCISE_STREAMING_ANALYTICS.md) | 30 min |
| 9 | Streaming JOINs in Flink SQL | 📖 + 🎥 | [MODULE_09](MODULE_09_STREAMING_JOINS.md) | 20 min |
| 10 | The Flink SQL Runtime | 📖 + 🎥 | [MODULE_10](MODULE_10_FLINK_SQL_RUNTIME.md) | 10 min |
| 11 | Pattern Recognition with MATCH_RECOGNIZE | 📖 + 🎥 | [MODULE_11](MODULE_11_PATTERN_RECOGNITION.md) | 19 min |
| 12 | **Match Recognize Exercise** | ✏️ Exercise | [MODULE_12](MODULE_12_EXERCISE_MATCH_RECOGNIZE.md) | 15 min |
| 13 | Changelog Processing | 📖 + 🎥 | [MODULE_13](MODULE_13_CHANGELOG_PROCESSING.md) | 10 min |
| 14 | **Stream Enrichment** | ✏️ Exercise | [MODULE_14](MODULE_14_EXERCISE_STREAM_ENRICHMENT.md) | 45 min |
| 15 | Using EXPLAIN for Troubleshooting | 📖 + 🎥 | [MODULE_15](MODULE_15_USING_EXPLAIN.md) | 19 min |


### Legende

- 📖 **Reading** = Theorie-Modul (Markdown-Dokumentation)
- 🎥 **Video** = YouTube Video eingebettet
- ✏️ **Exercise** = Praktische Übung (SQL Queries + Hands-on)

---

## 📋 Voraussetzungen

**Voraussetzungen für diese Demo:**

- ✅ Demo-Umgebung ist gestartet (`./start.sh` im Repo-Root)
- ✅ Confluent Platform 8.1.0 mit CP Flink
- ✅ Kubernetes Namespace: `confluent`
- ✅ Control Center: http://localhost:9021

---

## 🚀 Schnellstart

### Schritt 1: Demo-Umgebung starten

```bash
# Im confluent-platform-flink Repo (Root-Verzeichnis)
./start.sh
```

---

### Schritt 2: Test-Daten generieren

Die Exercises benötigen Test-Daten. Datagen-Clickstream und Datagen-Users benötigt in addition zu der Umgebung von Eike.

```bash
kubectl apply -f kubernetes/k8s-connector-datagen-orders.yaml (das sollte schon laufen, das wird über start.sh schon gemacht)

kubectl apply -f datagen-clickstream.yaml
kubectl apply -f datagen-users.yaml
```

**Was passiert:**
- `k8s-connector-datagen-orders.yaml` → Erstellt `orders` Topic (Datagen "orders" Quickstart)
- `datagen-clickstream.yaml` → Erstellt `clickstream` Topic (Datagen "clickstream" Quickstart)
- `datagen-users.yaml` → Erstellt `users` Topic (Datagen "users" Quickstart)

**Prüfen:**
```bash
# Connector Status
kubectl get connector -n confluent

# Topics
kubectl exec -n confluent kafka-0 -- kafka-topics --list \
  --bootstrap-server kafka.confluent.svc.cluster.local:9071 | grep -E "clickstream|users|orders"
```

**Erwartete Ausgabe:**
```
NAME                  STATUS    CONNECTORSTATUS   TASKS-READY
datagen-clickstream   CREATED   RUNNING           1/1
datagen-users         CREATED   RUNNING           1/1
orders                CREATED   RUNNING           1/1
```

---

### Schritt 3: Kurs starten

Start: [Modul 1: Overview](MODULE_01_OVERVIEW.md)

Oder springe direkt zu einem Exercise:
- **Watermarks:** [Modul 5](MODULE_05_EXERCISE_WATERMARKS.md)
- **Window Aggregations:** [Modul 8](MODULE_08_EXERCISE_STREAMING_ANALYTICS.md)
- **Pattern Recognition:** [Modul 12](MODULE_12_EXERCISE_MATCH_RECOGNIZE.md)
- **JOINs & Enrichment:** [Modul 14](MODULE_14_EXERCISE_STREAM_ENRICHMENT.md)

---

## 🛠️ Datagen Connector Details

### datagen-orders (orders)

Generiert Order-Daten (Quickstart: `orders`):
- Topic: `orders`
- Partitions: 1
- Format: Avro mit Schema Registry
- Rate: ~100ms zwischen Events
- Schema: ordertime, orderid, itemid, orderunits, address

### datagen-clickstream (clickstream)

Generiert Clickstream-Daten (Quickstart: `clickstream`):
- Topic: `clickstream`
- Partitions: 3
- Format: Avro mit Schema Registry
- Rate: ~100ms zwischen Events
- Schema: _time, time, ip, request, status, userid, bytes, agent

### datagen-users (users)

Generiert User-Daten (Quickstart: `users`):
- Topic: `users`
- Partitions: 1
- Format: Avro mit Schema Registry (Key + Value)
- Cleanup Policy: `compact` (nur neueste Version pro User)
- Rate: ~1000ms zwischen Events
- Schema: registertime, userid, regionid, gender

### Connector stoppen

```bash
# Einzeln löschen
kubectl delete connector datagen-clickstream -n confluent
kubectl delete connector datagen-users -n confluent
kubectl delete connector orders -n confluent

# Oder alle
kubectl delete -f datagen-clickstream.yaml
kubectl delete -f datagen-users.yaml
kubectl delete -f ../kubernetes/k8s-connector-datagen-orders.yaml
```

---

## 📚 Wichtige CP Flink Syntax-Unterschiede

### CP Flink Syntax-Besonderheiten

| Feature | CP Flink Syntax |
|---------|-----------------|
| **Connector** | `'connector' = 'confluent'` |
| **Format** | `'value.format' = 'avro-registry'` |
| **Tumbling Window** | `TABLE(TUMBLE(TABLE ...))` |
| **Upsert Key** | `'key.format' = 'avro-registry'` |

### Beispiel: Table Creation

```sql
CREATE TABLE orders (
    order_id STRING,
    price DECIMAL(10, 2)
) WITH (
    'connector' = 'confluent',
    'value.format' = 'avro-registry'
);
```

---

## 💡 Tipps

1. **Control Center nutzen:** Flink SQL Workspace zeigt Query-Ergebnisse in Echtzeit
2. **EXPLAIN verwenden:** Vor jedem INSERT prüfe den Execution Plan
3. **State TTL setzen:** Verhindert unbegrenztes State-Wachstum
4. **Watermarks prüfen:** `CURRENT_WATERMARK($rowtime)` zeigt aktuellen Watermark

---

## 📖 Weitere Ressourcen

- **Original-Kurs:** https://developer.confluent.io/courses/flink-sql/
- **Basis-Repo:** https://github.com/ethaden/confluent-platform-flink
- **Flink SQL Docs:** https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/table/sql
- **Confluent Docs:** https://docs.confluent.io/platform/current/flink/
