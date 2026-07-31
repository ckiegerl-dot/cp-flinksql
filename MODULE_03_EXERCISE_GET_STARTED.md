# Exercise: Get Started with CP Flink

**Original:** https://developer.confluent.io/courses/flink-sql/get-started-exercise/  
**Duration:** 30 min  
**Angepasst für:** Eikes Demo Environment (Confluent Platform 8.1.0 + CP Flink)

---

## 🎓 Lernziele

In dieser Übung lernst du:

- Demo-Umgebung überprüfen
- Flink SQL Workspace in Control Center nutzen
- Flink SQL Datenbanken und Tabellen verstehen
- Deine ersten Flink SQL Queries ausführen
- `$rowtime` und Event-Time Processing kennenlernen
- Built-in System-Funktionen erkunden
- Deine erste Tabelle erstellen und Daten einfügen

---

## Part 1: Verify Environment

### Check All Services Are Running

```bash
# Check Kafka
kubectl get pods -n confluent | grep kafka

# Check Flink
kubectl get pods -n confluent | grep flink

# Check Schema Registry
kubectl get pods -n confluent | grep schemaregistry

# Check Control Center
kubectl get pods -n confluent | grep control

# Check Connectors
kubectl get connector -n confluent
```

**Erwartete Ausgabe:** Alle Pods sollten `Running` Status haben.

---

### Access Control Center

1. Öffne Browser: http://localhost:9021
2. Login mit:
   - Username: `superUser`
   - Password: `superUser`
3. Du solltest das Control Center Dashboard sehen

**Troubleshooting:**

Falls Control Center nicht erreichbar:
```bash
# Port-Forwarding prüfen
kubectl port-forward -n confluent svc/controlcenter 9021:9021

# Logs prüfen
kubectl logs -n confluent controlcenter-0 --tail=50
```

---

## Part 2: Access Flink SQL Workspace

### Navigate to Flink SQL

1. In Control Center, klicke links im Menü auf **Flink (Preview)**
2. Klicke auf **SQL Workspace**
3. Du siehst jetzt den Flink SQL Editor

**Was du siehst:**

- **SQL Editor** (oben): Hier schreibst du Queries
- **Results** (unten): Query-Ergebnisse erscheinen hier
- **Schema Browser** (links): Zeigt verfügbare Tables

**⚙️ Wichtige Einstellungen für SQL Statements:**

Bevor du Queries ausführst, stelle sicher dass folgende Werte gesetzt sind:

- **Statement Type:** `Flink SQL statement`
- **Compute Pool:** `mypool` (oder `*` für default)
- **Catalog:** `mycatalog`
- **Database:** `mykafka`

**Hinweis:** Diese Settings sind normalerweise voreingestellt, aber prüfe sie bei Fehlern.

---

### Understanding Flink SQL Hierarchy

Flink SQL organisiert Daten in einer Hierarchie:

```
Catalog
  └─ Database
      └─ Table
```

**In dieser Demo:**

```sql
-- Zeige alle Databases
SHOW DATABASES;
```

**Erwartete Ausgabe:**
```
default
```

**Aktuell nutzen wir die `default` Database** - alle Tables die wir erstellen landen hier.

---

## Part 3: Your First Flink SQL Queries

### Create a Simple Test Table

```sql
CREATE TABLE hello_flink (
    id INT,
    message STRING,
    event_time TIMESTAMP(3),
    WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
) WITH (
    'connector' = 'confluent',
    'value.format' = 'avro-registry'
);
```

**Was passiert hier?**

- Flink erstellt ein **Kafka Topic** namens `hello_flink`
- Schema wird in **Schema Registry** registriert (Avro Format)
- Watermark wird auf `event_time` definiert (5 Sekunden Toleranz)

---

### Insert Test Data

```sql
INSERT INTO hello_flink VALUES
    (1, 'Hello from Flink SQL!', CURRENT_TIMESTAMP),
    (2, 'Running on cp-demo', CURRENT_TIMESTAMP),
    (3, 'Confluent Platform 8.1.0', CURRENT_TIMESTAMP);
```

**🔍 Starte das INSERT Statement!**

**Beobachtung:** In Control Center siehst du das Statement als "Running" - es läuft kontinuierlich.

---

### Query the Table

```sql
SELECT * FROM hello_flink;
```

**🔍 Teste das Query!**

**Erwartete Ausgabe:**
```
+----+--------------------------+-------------------------+
| id | message                  | event_time              |
+----+--------------------------+-------------------------+
| 1  | Hello from Flink SQL!    | 2026-07-31T14:23:01.234 |
| 2  | Running on cp-demo       | 2026-07-31T14:23:01.234 |
| 3  | Confluent Platform 8.1.0 | 2026-07-31T14:23:01.234 |
+----+--------------------------+-------------------------+
```

**Wichtig:** Dies ist ein **Streaming Query** - es läuft kontinuierlich und zeigt neue Daten automatisch!

---

## Part 4: Understanding $rowtime

Jede Flink Table hat ein **verstecktes System-Feld** namens `$rowtime`.

### View $rowtime

```sql
SELECT id, message, event_time, $rowtime FROM hello_flink;
```

**🔍 Teste das Query!**

**Beobachtungen:**

- `event_time` = Der Timestamp den **wir** ins Feld geschrieben haben
- `$rowtime` = Der **Kafka Record Timestamp** (automatisch gesetzt)

**Unterschied:**

- `event_time`: Unser Application-Timestamp (wann das Event **passiert** ist)
- `$rowtime`: Kafka-Timestamp (wann das Event **ins Topic geschrieben** wurde)

---

### The Importance of $rowtime

`$rowtime` ist das **Time Attribute** für:

- **Watermarks** (Event-Time Processing)
- **Window Aggregations** (TUMBLE, HOP, etc.)
- **Time-based JOINs**
- **Sorting** (ORDER BY funktioniert nur auf Time Attributes!)

**Example - Sorting:**

```sql
SELECT id, message, $rowtime 
FROM hello_flink 
ORDER BY $rowtime;
```

**🔍 Teste das Query!**

**Was passiert:** Flink sortiert den Stream basierend auf `$rowtime`.

---

## Part 5: Explore System Functions

### CURRENT_TIMESTAMP

```sql
SELECT CURRENT_TIMESTAMP AS now;
```

**Output:** Aktueller Timestamp (wiederholt sich, da kontinuierlicher Stream).

---

### CURRENT_WATERMARK

```sql
SELECT 
    id, 
    message, 
    $rowtime,
    CURRENT_WATERMARK($rowtime) AS wm
FROM hello_flink;
```

**🔍 Teste das Query!**

**Beobachtung:**

- Watermark startet bei `NULL`
- Nach einigen Events springt der Watermark vorwärts
- Watermark liegt immer **5 Sekunden hinter** dem neuesten Event (wegen `INTERVAL '5' SECOND` in der Table Definition)

---

### DESCRIBE EXTENDED

```sql
DESCRIBE EXTENDED hello_flink;
```

**🔍 Teste das Query!**

**Output zeigt:**

- Alle Spalten (auch `$rowtime`)
- Data Types
- Watermark Strategy
- Connector Properties

**Wichtig:** `$rowtime` wird als `METADATA VIRTUAL` angezeigt = Read-only System-Feld.

---

## Part 6: Verify Kafka Topic

Flink hat automatisch ein Kafka Topic erstellt. Lass uns das verifizieren:

```bash
# Liste alle Topics
kubectl exec -n confluent kraftcontroller-0 -- kafka-topics --list \
  --bootstrap-server kafka.confluent.svc.cluster.local:9071 | grep hello

# Topic Details anzeigen
kubectl exec -n confluent kraftcontroller-0 -- kafka-topics --describe \
  --bootstrap-server kafka.confluent.svc.cluster.local:9071 \
  --topic hello_flink
```

**Erwartete Ausgabe:**
```
Topic: hello_flink
PartitionCount: 1
ReplicationFactor: 1
```

---

### Consume from Kafka Topic

```bash
kubectl exec -n confluent kafka-0 -- kafka-console-consumer \
  --bootstrap-server kafka.confluent.svc.cluster.local:9071 \
  --topic hello_flink \
  --from-beginning \
  --max-messages 3
```

**Output:** Avro-binäre Daten (nicht lesbar) - Schema Registry wird verwendet!

---

## Part 7: Cleanup

### Stop INSERT Statement

**In Control Center:**

1. Gehe zu **Flink (Preview)** → **Jobs**
2. Finde das INSERT Statement
3. Klicke **Stop**

---

### Delete Table

```sql
DROP TABLE IF EXISTS hello_flink;
```

---

### Delete Kafka Topic

```bash
kubectl exec -n confluent kraftcontroller-0 -- kafka-topics --delete \
  --bootstrap-server kafka.confluent.svc.cluster.local:9071 \
  --topic hello_flink
```

---

## 🎯 Key Takeaways

Nach diesem Exercise verstehst du:

- ✅ Wie man **Control Center** und **Flink SQL Workspace** nutzt
- ✅ Wie man **Tables erstellt** mit `CREATE TABLE`
- ✅ Wie man **Daten inserted** mit `INSERT INTO`
- ✅ Wie man **Queries ausführt** mit `SELECT`
- ✅ Was **`$rowtime`** ist (Kafka Record Timestamp = Time Attribute)
- ✅ Was **Watermarks** sind und wie sie in Table Definitions erscheinen
- ✅ Flink SQL ist **Streaming-first**: Queries laufen kontinuierlich!
- ✅ Flink erstellt automatisch **Kafka Topics** und **Avro Schemas**

---

## 📚 Next Steps

Jetzt bist du bereit für die weiteren Exercises:

1. **Exercise 1:** [Watermarks](MODULE_05_EXERCISE_WATERMARKS.md) - Deep Dive in Event-Time Processing
2. **Exercise 2:** [Streaming Analytics](MODULE_08_EXERCISE_STREAMING_ANALYTICS.md) - Window Aggregations
3. **Exercise 3:** [Match Recognize](MODULE_12_EXERCISE_MATCH_RECOGNIZE.md) - Pattern Recognition
4. **Exercise 4:** [Stream Enrichment](MODULE_14_EXERCISE_STREAM_ENRICHMENT.md) - JOINs & Temporal Tables

---

**Viel Erfolg! 🚀**
