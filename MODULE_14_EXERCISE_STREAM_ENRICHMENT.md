# Exercise: Stream Enrichment (CP Flink Angepasst)

**Original:** https://developer.confluent.io/courses/flink-sql/exercise-stream-enrichment/  
**Duration:** 45 min  
**Instructor:** David Anderson, Principal Software Practice Lead

**Angepasst für:** Eikes Demo Environment (Confluent Platform 8.1.0 + CP Flink)

---

## 🎓 Lernziele

- Temporal Joins für Stream Enrichment
- Versioned Tables vs. Append-only Streams
- UNION ALL + OVER Windows für Stream Kombination
- Upsert vs. Append-only Materialization

---

## Konzept: Temporal Joins

**Temporal Join = Stream Enrichment Pattern**

```
Append-only Stream  +  Versioned Table  →  Enriched Stream
   (Events)              (Lookups)           (Events + Context)
```

**Vorteil:** Append-only Output → effizient!

**Basic Temporal Join Example:**

```sql
SELECT 
    CAST(o.orderid AS STRING) AS customer_id,
    o.orderunits AS price,
    u.regionid AS region
FROM orders o
JOIN users FOR SYSTEM_TIME AS OF o.event_time
    ON CAST(o.orderid AS STRING) = u.userid;
```

---

## 🚀 Setup: Customers Table erstellen

**Voraussetzung:** `orders` Table existiert (aus Exercise 2/3).

### Step 1: Datagen Connector starten

```bash
kubectl apply -f datagen-users.yaml
```

Prüfen:
```bash
kubectl get connector datagen-users -n confluent
kubectl get topic users -n confluent
```

### Step 2: Users Table erstellen (Datagen Schema)

**In Control Center → Flink SQL:**

```sql
CREATE TABLE users (
    `registertime` BIGINT,
    `userid` STRING,
    `regionid` STRING,
    `gender` STRING,
    `event_time` AS TO_TIMESTAMP_LTZ(`registertime`, 3),
    WATERMARK FOR `event_time` AS `event_time` - INTERVAL '5' SECOND,
    PRIMARY KEY (`userid`) NOT ENFORCED
) WITH (
    'connector' = 'confluent',
    'changelog.mode' = 'upsert',
    'key.format' = 'avro-registry',
    'value.format' = 'avro-registry'
);
```

**Was passiert:**
- Liest vom `users` Topic (Datagen "users" Quickstart)
- **Table-Name = Topic-Name** (CP Flink Anforderung!)
- `registertime` (BIGINT Millis) → `event_time` (TIMESTAMP_LTZ)
- `PRIMARY KEY (userid)` für Upsert-Semantik
- `changelog.mode = 'upsert'` = Versioned Table

### Step 3: BA-semantische VIEW erstellen (optional)

```sql
CREATE VIEW kunden AS
SELECT 
    `userid` AS kunden_id,
    `regionid` AS region,
    `gender` AS geschlecht,
    `event_time` AS registrierungs_zeitpunkt
FROM users;
```

**BA-Kontext:**
- `userid` → Kunden-ID (Arbeitssuchender)
- `regionid` → Region/Agentur
- `registertime` → Registrierungszeitpunkt

---

## Exercise 1: Enrich MATCH_RECOGNIZE Output

**Voraussetzung:** Orders Table muss existieren (siehe MODULE_08 oder MODULE_12 Setup).

**Szenario:** Customer macht 3 Käufe mit steigenden Preisen → Region/Gender hinzufügen!

### Step 1: Pattern - 3 steigende Preise

```sql
WITH customers_up_and_up AS (
    SELECT *
    FROM orders
    MATCH_RECOGNIZE (
        PARTITION BY CAST(orderid AS STRING)
        ORDER BY event_time
        MEASURES
            CAST(orderid AS STRING) AS customer_id,
            SUM(UP.orderunits) AS total_spent,
            event_time AS match_time
        AFTER MATCH SKIP PAST LAST ROW
        PATTERN (UP{3})
        DEFINE
            UP AS (COUNT(UP.orderunits) = 1) OR 
                  (UP.orderunits > LAST(UP.orderunits, 1))
    )
)
SELECT * FROM customers_up_and_up;
```

**🔍 Teste das Query!**

**Pattern:** Finde Kunden die 3 Käufe mit steigenden Preisen machten.

**⚠️ CP Flink Limitation:** `MATCH_ROWTIME()` funktioniert nicht → nutze reguläre `order_time` Spalte!

---

### Step 2: Temporal Join - Region hinzufügen

```sql
WITH customers_up_and_up AS (
    SELECT *
    FROM orders
    MATCH_RECOGNIZE (
        PARTITION BY CAST(orderid AS STRING)
        ORDER BY event_time
        MEASURES
            CAST(orderid AS STRING) AS customer_id,
            SUM(UP.orderunits) AS total_spent,
            event_time AS match_time
        AFTER MATCH SKIP PAST LAST ROW
        PATTERN (UP{3})
        DEFINE
            UP AS (COUNT(UP.orderunits) = 1) OR 
                  (UP.orderunits > LAST(UP.orderunits, 1))
    )
)
SELECT 
    c.customer_id,
    c.total_spent,
    u.regionid AS region,
    u.gender AS geschlecht
FROM customers_up_and_up c
JOIN users FOR SYSTEM_TIME AS OF c.match_time
    ON c.customer_id = u.userid;
```

**🔍 Teste das Query!**

**Schlüssel:** `FOR SYSTEM_TIME AS OF match_time` = Temporal Join!

**Erwartung:** Customer IDs mit `total_spent` + `regionid` + `gender` (falls Pattern matched).

---

## Enriching with Append-Only Streams

**Szenario:** Kombiniere 2 Append-only Streams (`orders` + `clicks`) um comprehensive Customer Profile zu bauen.

### Setup: Clicks Table erstellen

**Nutze clickstream aus MODULE_05**

Die Clickstream-Daten wurden bereits in MODULE_05 eingerichtet:

```sql
-- Falls noch nicht vorhanden:
CREATE TABLE clickstream (
    `_time` BIGINT,
    `ip` STRING,
    `request` STRING,
    `userid` INT,
    `event_time` AS TO_TIMESTAMP_LTZ(`_time`, 3),
    WATERMARK FOR `event_time` AS `event_time` - INTERVAL '5' SECOND
) WITH (
    'connector' = 'confluent',
    'value.format' = 'avro-registry'
);

CREATE VIEW onlineportal_klicks AS
SELECT 
    CAST(`userid` AS STRING) AS arbeitssuchender_id,
    `request` AS angeklickte_url,
    `event_time` AS klick_zeitpunkt
FROM clickstream;
```

---

### Exercise 2: Unified Customer View

**Step 1: Unify Schemas mit UNION ALL**

```sql
CREATE VIEW combined_customer_data AS (
    SELECT
        CAST(orderid AS STRING) AS customer_id,
        itemid AS product_id,
        orderunits AS price,
        CAST(NULL AS STRING) AS url,
        event_time
    FROM orders
    UNION ALL
    SELECT
        CAST(`userid` AS STRING) AS customer_id,
        CAST(NULL AS STRING) AS product_id,
        CAST(NULL AS DOUBLE) AS price,
        `request` AS url,
        `event_time` as event_time
    FROM clickstream
);
```

**Konzept:** NULL-Spalten für fehlende Felder → Unified Schema.

**🔍 Teste die View:**

```sql
SELECT * FROM combined_customer_data LIMIT 20;
```

---

**Step 2: Track Latest Values mit OVER Aggregation**

```sql
CREATE VIEW latest_customer_info AS (
    SELECT
        LAST_VALUE(customer_id) OVER w AS customer_id,
        LAST_VALUE(product_id) OVER w AS product_id,
        LAST_VALUE(price) OVER w AS price,
        LAST_VALUE(url) OVER w AS url,
        MAX(event_time) OVER w AS event_time
    FROM combined_customer_data
    WINDOW w AS (
        PARTITION BY customer_id
        ORDER BY event_time
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    )
);
```

**Konzept:** OVER Window sammelt alle Werte → `LAST_VALUE` gibt neuesten.

**🔍 Teste die View:**

```sql
SELECT * FROM latest_customer_info 
WHERE customer_id IN ('3001', '3002', '3003')
LIMIT 20;
```

**Erwartung:** Für jeden Customer die neuesten bekannten Werte aus beiden Streams!

---

## Materialization Options

### Option 1: Append-Only (Full History)

```sql
CREATE TABLE latest_customer_info_appending AS (
    SELECT * FROM latest_customer_info
);
```

**🔍 Query the Table:**

```sql
SELECT * FROM latest_customer_info_appending LIMIT 20;
```

**Topic:** Alle Events behalten → kann groß werden!

**Use Case:** Vollständige Historie für Audit/Analytics.

---

### Option 2: Upserting (Compacted)

```sql
CREATE TABLE latest_customer_info_upserting (
    PRIMARY KEY (customer_id) NOT ENFORCED
) WITH (
    'changelog.mode' = 'upsert',
    'key.format' = 'avro-registry',
    'value.format' = 'avro-registry',
    'kafka.cleanup-policy' = 'compact'
) AS (
    SELECT * FROM latest_customer_info
);
```

**🔍 Query the Table:**

```sql
SELECT * FROM latest_customer_info_upserting LIMIT 20;
```

**Topic:** Nur neueste Version pro Key → kompakt!

**Use Case:** Aktuelle 360° Customer View für ML/Personalization.

---

## Cleanup

**In Control Center → Flink SQL:**

```sql
-- Views löschen
DROP VIEW IF EXISTS combined_customer_data;
DROP VIEW IF EXISTS latest_customer_info;
DROP VIEW IF EXISTS kunden;
DROP VIEW IF EXISTS onlineportal_klicks;

-- Tables löschen
DROP TABLE IF EXISTS latest_customer_info_appending;
DROP TABLE IF EXISTS latest_customer_info_upserting;
```

---

## 🎯 Summary

Nach diesem Exercise verstehst du:

- ✅ Temporal Joins (`FOR SYSTEM_TIME AS OF`)
- ✅ Versioned Tables (`changelog.mode = 'upsert'`)
- ✅ UNION ALL für Stream-Kombination
- ✅ OVER Windows für Gradual Accumulation
- ✅ Append-only vs. Upsert Materialization
- ✅ PRIMARY KEY + `key.format` für Upsert Tables

---

## 📚 Bonus: Real-World Pattern

**360° Customer View:**

```
Orders + Clicks + Support-Tickets + Emails
    ↓
UNION ALL + OVER Windows
    ↓
Comprehensive Customer Profile
```

**Use Cases:**
- ML Features für Recommendations
- Churn Prediction
- Personalization
- Real-Time Segmentation

---

## 📚 Resources

- [Temporal Joins (Apache Flink Docs)](https://nightlies.apache.org/flink/flink-docs-release-1.19/docs/dev/table/sql/queries/joins/#temporal-joins)
- [Versioned Tables (Confluent Docs)](https://docs.confluent.io/cloud/current/flink/reference/statements/create-table.html)
- [Stream Enrichment Tutorial](https://developer.confluent.io/courses/flink-sql/stream-enrichment/)

---

**Das war's! Alle Exercises sind fertig! 🎉**

**Exercise-Reihenfolge:**
1. [Exercise 2: Streaming Analytics](MODULE_08_EXERCISE_STREAMING_ANALYTICS.md)
2. [Exercise 3: MATCH_RECOGNIZE](MODULE_12_EXERCISE_MATCH_RECOGNIZE.md)
3. [Exercise 4: Stream Enrichment](MODULE_14_EXERCISE_STREAM_ENRICHMENT.md)
