# Exercise: Hands-on with Watermarks (CP Flink Angepasst)

**Original:** https://developer.confluent.io/courses/flink-sql/watermarks-exercise/  
**Duration:** 30 min  
**Instructor:** David Anderson, Principal Software Practice Lead

**Angepasst für:** Eikes Demo Environment (Confluent Platform 8.1.0 + CP Flink)

---

## 🎓 Lernziele

In dieser Übung lernst du:

- Die Default-Watermarking-Strategie in CP Flink nutzen
- Eigene Watermarking-Strategien definieren und wann das sinnvoll ist
- Queries debuggen die keine Ergebnisse liefern
- Late Events erkennen
- Partition-Anzahl für Kafka Topics konfigurieren
- Wie Sorting in Flink SQL funktioniert
- Kafka Record Timestamps lesen und schreiben
- Auswirkungen von leeren/idle Partitions verstehen
- CASE Statements nutzen
- Common Table Expressions (CTEs) verwenden

---

## 🚀 Setup

### Voraussetzungen

- ✅ Demo-Umgebung läuft (`./start.sh`)
- ✅ Control Center erreichbar: http://localhost:9021
- ✅ Test-Daten Connectors laufen

### Step 1: Datagen Connector starten

```bash
kubectl apply -f datagen-clickstream.yaml
```

Prüfen:
```bash
kubectl get connector datagen-clickstream -n confluent
kubectl get topic clickstream -n confluent
```

### Step 2: Flink Table erstellen (Datagen Schema)

**In Control Center → Flink SQL:**

```sql
CREATE TABLE clickstream (
    `_time` BIGINT,
    `time` STRING,
    `ip` STRING,
    `request` STRING,
    `status` INT,
    `userid` INT,
    `bytes` BIGINT,
    `agent` STRING,
    `event_time` AS TO_TIMESTAMP_LTZ(`_time`, 3),
    WATERMARK FOR `event_time` AS `event_time` - INTERVAL '5' SECOND
) WITH (
    'connector' = 'confluent',
    'value.format' = 'avro-registry'
);
```

**Was passiert:**
- Liest vom `clickstream` Topic (Datagen "clickstream" Quickstart)
- **Table-Name = Topic-Name** (CP Flink Anforderung!)
- `_time` (BIGINT Millis) → `event_time` (TIMESTAMP_LTZ) konvertiert
- Watermark auf konvertiertem Timestamp

### Step 3: BA-semantische VIEW erstellen (optional)

```sql
CREATE VIEW onlineportal_klicks AS
SELECT 
    CAST(`userid` AS STRING) AS arbeitssuchender_id,
    `request` AS angeklickte_url,
    `ip` AS nutzer_ip,
    `status` AS http_status,
    `event_time` AS klick_zeitpunkt
FROM clickstream;
```

**BA-Kontext:**
- `userid` → Arbeitssuchender-ID
- `request` → URL im BA-Onlineportal
- `event_time` → Klick-Zeitpunkt

---

## Part 1: Using Default Watermarks

### Examine the Clickstream Table

```sql
DESCRIBE EXTENDED clickstream;
```

**🔍 Teste das Query!**

**Erwartete Ausgabe:**
```
+----------------+-----------------------------+----------+------------------------------+---------+
| Column Name    | Data Type                   | Nullable | Extras                       | Comment |
+----------------+-----------------------------+----------+------------------------------+---------+
| _time          | BIGINT                      | TRUE     |                              |         |
| ip             | STRING                      | TRUE     |                              |         |
| request        | STRING                      | TRUE     |                              |         |
| userid         | INT                         | TRUE     |                              |         |
| event_time     | TIMESTAMP_LTZ(3) *ROWTIME*  | NOT NULL | AS TO_TIMESTAMP_LTZ..., WATERMARK | COMPUTED |
| $rowtime       | TIMESTAMP_LTZ(3) *ROWTIME*  | NOT NULL | METADATA VIRTUAL, WATERMARK  | SYSTEM  |
+----------------+-----------------------------+----------+------------------------------+---------+
```

**Key Points:**
- `$rowtime` ist das **versteckte System-Feld** (METADATA VIRTUAL = read-only)
- `*ROWTIME*` annotation zeigt: Dies ist das **Event-Time Attribute**
- **WATERMARK** ist darauf definiert
- Assumption: Events kommen **ungefähr in Order** bezüglich `$rowtime`

---

### Sort by $rowtime

```sql
SELECT userid, request, $rowtime
FROM clickstream
ORDER BY $rowtime;
```

**🔍 Teste das Query!**

**Erwartete Ausgabe:**
```
+---------+----------------------+-------------------------+
| userid  | request              | $rowtime                |
+---------+----------------------+-------------------------+
| 1023    | GET /index.html      | 2026-07-31T14:23:01.234 |
| 1045    | GET /jobs            | 2026-07-31T14:23:01.567 |
| 1012    | GET /profile         | 2026-07-31T14:23:02.123 |
...
```

**Beobachtung:** Ergebnisse sind **nach Timestamp sortiert**. Das funktioniert, weil Watermarks Flink erlauben, den Stream zu buffern bis alle Events eines Zeitfensters angekommen sind.

---

### Try Sorting by Another Field (Dies wird fehlschlagen!)

```sql
SELECT userid, request, $rowtime
FROM clickstream
ORDER BY userid;
```

**🔍 Teste das Query!**

**Erwarteter Fehler:**
```
SQL compilation error: Sort on a non-time-attribute field is not supported.
```

**Erklärung:** Flink SQL kann unbegrenzte Streams **nicht** nach beliebigen Feldern sortieren - das würde unbegrenztes Buffering benötigen. Sorting funktioniert **nur auf Time Attributes**.

---

### View Watermarks

```sql
SELECT userid, request, $rowtime, CURRENT_WATERMARK($rowtime) AS wm
FROM clickstream 
LIMIT 500;
```

**🔍 Teste das Query!**

**Erwartete Beobachtungen:**

1. **Anfangs:** Watermark ist **NULL**
2. **Während Processing:** Watermark bleibt für mehrere Zeilen **konstant**
3. **Dann:** Watermark **springt** plötzlich vorwärts

**Warum?**

Flink generiert Watermarks **periodisch** (alle 200 Millisekunden), nicht bei jedem Event. Daher sehen mehrere aufeinanderfolgende Events denselben Watermark-Wert.

---

## Part 2: Create Experimental Table with Custom Configuration

### Create Table with 2 Partitions

```sql
CREATE TABLE some_clicks (
    partition_key INT,
    userid INT NOT NULL,
    request STRING NOT NULL,
    event_time TIMESTAMP(3) METADATA FROM 'timestamp'
) DISTRIBUTED BY HASH(partition_key) INTO 2 BUCKETS;
```

**Key Configuration Details:**

- `DISTRIBUTED BY HASH(partition_key) INTO 2 BUCKETS` = **2-Partition Kafka Topic**
- `event_time TIMESTAMP(3) METADATA FROM 'timestamp'` = mappt **Kafka Record Timestamp** auf ein **schreibbares Feld** (nicht read-only wie `$rowtime`)
- Wir werden absichtlich **eine leere Partition** erstellen zum Testen

---

### Populate the Table

```sql
INSERT INTO some_clicks
SELECT
    1 AS partition_key, 
    userid,
    request,
    $rowtime AS event_time
FROM clickstream 
LIMIT 500;
```

**🔍 Starte das INSERT Statement!**

**Effekt:** Alle 500 Records gehen in **dieselbe Partition** (partition_key=1), die andere Partition bleibt **leer (idle)**.

---

## Part 3: Idleness Experiments

### Sort the New Table

```sql
SELECT
    user_id, 
    url, 
    event_time, 
    CURRENT_WATERMARK(event_time) AS wm
FROM some_clicks
ORDER BY event_time;
```

**🔍 Teste das Query!**

**Erwartetes Verhalten:**

1. **Delay** bevor Ergebnisse erscheinen (warten auf Idle Timeout der leeren Partition)
2. Watermark zeigt **NULL** während der gesamten Ausgabe (Daten wurden bereits komplett verarbeitet bevor Timeout)
3. **Progressive Idleness:** Timeout startet bei wenigen Sekunden, steigt bis maximal **5 Minuten**

**Warum der Delay?**

Eine Partition ist leer → Flink wartet darauf dass Events kommen → nach Idle Timeout wird die Partition als "idle" markiert → Query kann fortfahren.

---

### Configure Custom Idle Timeout

```sql
SET 'sql.tables.scan.idle-timeout' = '1 seconds';

SELECT
    user_id, 
    url, 
    event_time, 
    CURRENT_WATERMARK(event_time) AS wm
FROM some_clicks
ORDER BY event_time;
```

**🔍 Teste das Query mit verschiedenen Timeout-Werten!**

**Beobachtungen:**

- `'1 seconds'` → Ergebnisse erscheinen **schneller**
- `'10 seconds'` → Längerer Delay
- `'0 seconds'` → **Deaktiviert Idle Detection** (Query hängt **für immer** bei leeren Partitions!)

**Best Practice:**

- **Ad-hoc Queries / Testing:** 1-5 Sekunden
- **Production:** 1-5 Minuten (je nach Use Case)

---

## Part 4: Detecting Late Events

### Create Out-of-Order Table

```sql
CREATE TABLE ooo_clicks (
    userid INT NOT NULL,
    request STRING NOT NULL,
    event_time TIMESTAMP(3) METADATA FROM 'timestamp',
    WATERMARK FOR event_time AS event_time - INTERVAL '1' SECOND
);
```

**Configuration:** Watermark erlaubt nur **1 Sekunde** Out-of-Order-ness!

---

### Populate with Randomized Timestamps

```sql
INSERT INTO ooo_clicks
SELECT
    userid,
    request,
    TIMESTAMPADD(SECOND, MOD(ABS(CAST(RAND() * 100 AS INT)), 6), $rowtime) AS event_time
FROM clickstream;
```

**🔍 Starte das INSERT Statement!**

**Effekt:**

- Timestamps werden um **0-5 Sekunden** nach vorne verschoben
- Events sind bis zu **5 Sekunden out-of-order**
- Aber Watermark erlaubt nur **1 Sekunde** → viele Events werden **late** sein!

**Hinweis:** CP Flink hat kein `RAND_INTEGER(6)` - daher nutzen wir `MOD(ABS(CAST(RAND() * 100 AS INT)), 6)` als Equivalent.

---

### Count Late Events (CTE Version)

```sql
WITH a_thousand_clicks AS 
    (SELECT * FROM ooo_clicks LIMIT 1000) 
SELECT COUNT(*) 
FROM a_thousand_clicks 
WHERE event_time <= CURRENT_WATERMARK(event_time);
```

**🔍 Teste das Query mehrmals!**

**Erwartung:**

- Count der Late Events unter den ersten 1000
- Falls **keine** Late Events: Query gibt **kein Ergebnis** zurück (nicht Null, sondern gar nichts!)
- **Non-Deterministisch:** Jede Ausführung kann unterschiedliche Ergebnisse liefern

---

### Count Late Events (Running Total)

```sql
SELECT
    COUNT(*) AS total_events,
    SUM(
        CASE WHEN event_time <= CURRENT_WATERMARK(event_time) THEN 1 
        ELSE 0 
        END
    ) AS late_events
FROM ooo_clicks;
```

**🔍 Teste das Query!**

**Erwartete Ausgabe:** Kontinuierlicher Stream mit total_events und late_events Count.

**Key Observations:**

1. **Late Events sind non-deterministisch** - wiederhole die Query mehrmals, du siehst verschiedene Ergebnisse
2. **Weniger Late Events als erwartet** - Watermark wird alle 200ms aktualisiert, nicht bei jedem Event
3. **~10 Events zwischen Watermarks** in Real-Time (bei 50 Events/Sekunde)
4. **Historische Daten** werden schneller verarbeitet → viele Events zwischen Watermarks → weniger Late Events
5. **Nach ersten 5 Sekunden** (250 Events bei 50/sec) können **keine Events mehr late sein**

**Trick: CASE für Conditional Aggregation:**

```sql
SUM(CASE WHEN <condition> THEN 1 ELSE 0 END)
```

Dies ist ein **gängiges Pattern** für bedingte Aggregationen in SQL!

---

## Part 5: CURRENT_WATERMARK() and Updating Tables

⚠️ **Wichtige Einschränkung:**

`CURRENT_WATERMARK()` funktioniert **NUR** auf **Append-only Tables**!

**Append-only Table:**
```sql
CREATE TABLE clicks (...);  -- Kein PRIMARY KEY
```

**Updating Table (Upsert):**
```sql
CREATE TABLE customers (
    customer_id INT NOT NULL,
    name STRING NOT NULL,
    address STRING NOT NULL,
    postcode STRING NOT NULL,
    city STRING NOT NULL,
    email STRING NOT NULL,
    PRIMARY KEY (customer_id) NOT ENFORCED
);
```

**Fehler bei CURRENT_WATERMARK() auf Updating Table:**
```
Non-deterministic function used with update messages not supported.
```

**Alternative für Debugging:**

In Control Center kannst du **Metrics/Graphs** für Flink Jobs sehen:
- Input/Output Rate
- Consumer Lag
- Watermark Progression

---

## Cleanup

### Stop Running INSERT Statements

**In Control Center:**

1. Gehe zu **Flink (Preview)** → **Jobs**
2. Finde die laufenden INSERT Statements
3. Klicke **Stop** für jedes Statement

### Delete Tables

```sql
DROP TABLE IF EXISTS clickstream;
DROP VIEW IF EXISTS onlineportal_klicks;
DROP TABLE IF EXISTS some_clicks;
DROP TABLE IF EXISTS ooo_clicks;
```

### Delete Connector und Topic (Optional)

```bash
kubectl delete -f datagen-clickstream.yaml
```

---

## 🎯 Key Takeaways

Nach diesem Exercise verstehst du:

1. **Default watermarking** in CP Flink funktioniert für die meisten Use Cases
2. **Definiere Custom Watermarks** wenn:
   - Topic hat sehr wenig Daten
   - Default produziert zu viele Late Events
   - Du höhere Latenz in Kauf nimmst um Late Events zu reduzieren
3. **Custom Idle Timeout** setzen wenn du idle Topics hast und niedrigere Latenz brauchst
4. **CURRENT_WATERMARK()** funktioniert nur mit Append-only Tables im interaktiven Modus
5. **Late Events sind non-deterministisch** und häufiger bei Real-Time als bei historischen Daten
6. **Progressive Idleness** startet bei Sekunden, steigt bis 5 Minuten Maximum

---

## 📚 Resources

- [Time and Watermarks (Confluent Docs)](https://docs.confluent.io/cloud/current/flink/concepts/timely-stream-processing.html)
- [Watermarks Tutorial](https://developer.confluent.io/courses/flink-sql/watermarks-exercise/)
- [Flink Watermarks (Apache Docs)](https://nightlies.apache.org/flink/flink-docs-release-1.19/docs/dev/datastream/event-time/generating_watermarks/)

---

**Weiter zu:** [Exercise 2: Streaming Analytics](MODULE_08_EXERCISE_STREAMING_ANALYTICS.md)
