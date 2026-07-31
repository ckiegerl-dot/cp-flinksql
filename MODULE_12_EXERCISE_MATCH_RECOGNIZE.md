# Exercise: MATCH_RECOGNIZE (CP Flink Angepasst)

**Original:** https://developer.confluent.io/courses/flink-sql/match-recognize-exercise/  
**Duration:** 15-20 min  
**Instructor:** David Anderson, Principal Software Practice Lead

**Angepasst für:** Eikes Demo Environment (Confluent Platform 8.1.0 + CP Flink)

---

## 🎓 Lernziele

- Resource-effiziente MATCH_RECOGNIZE Patterns schreiben
- Greedy vs. Reluctant Quantifiers verstehen
- State-Wachstum begrenzen (WITHIN, Upper Limits)
- Real-World Use Case: Event-Journey bei der BA

---

## 🚀 Setup

**Voraussetzung:** Das `orders` Topic aus Exercise 2 existiert bereits und enthält Daten.

### Orders Table und VIEW erstellen

**Table sollte bereits existieren (aus MODULE_08). Falls nicht:**

```sql
CREATE TABLE orders (
    `ordertime` BIGINT,
    `orderid` INT,
    `itemid` STRING,
    `orderunits` DOUBLE,
    `address` ROW<`city` STRING, `state` STRING, `zipcode` BIGINT>,
    `event_time` AS TO_TIMESTAMP_LTZ(`ordertime`, 3),
    WATERMARK FOR `event_time` AS `event_time` - INTERVAL '5' SECOND
) WITH (
    'connector' = 'confluent',
    'value.format' = 'avro-registry'
);
```

**VIEW für MATCH_RECOGNIZE (PARTITION BY braucht einfache Spalten!):**

```sql
CREATE VIEW orders AS
SELECT 
    CAST(orderid AS STRING) AS customer_id,
    itemid AS product_id,
    orderunits AS price,
    TO_TIMESTAMP_LTZ(ordertime, 3) AS order_time,
    ordertime,
    orderid,
    address
FROM orders;
```

**⚠️ Wichtig:** Wir müssen `TO_TIMESTAMP_LTZ(ordertime, 3)` neu berechnen, weil berechnete Spalten (`event_time`) nicht in VIEWs referenziert werden können.

**BA-Kontext Interpretation:**
- `orderid` → `customer_id` (Arbeitssuchender-ID)  
- `itemid` → `product_id` (Event-Type im Vermittlungsprozess)
- `orderunits` → `price` (Wert)
- Berechneter `order_time` Timestamp für Pattern Matching

---

## Best Practices for Avoiding Resource Exhaustion

When patterns use optional or iterable quantifiers (`*`, `+`, `?`, `{n,m}`), Flink stores **active potential matches** in state → kann SEHR groß werden!

### Key Guidelines ⚠️

**Vermeide:**
- ❌ Variablen die auf jede Zeile matchen (`TRUE`)
- ❌ Patterns ohne definitives Ende (`O+` am Ende)
- ❌ Unbegrenzte Quantifiers (`*`, `+`) ohne Upper Limit

**Nutze:**
- ✅ Definitive Pattern-Endings
- ✅ Upper Quantifier Limits: `{,4}` statt `*`
- ✅ Time Constraints: `WITHIN INTERVAL '2' MINUTES`

---

## Greedy vs. Reluctant Quantifiers

| Quantifier | Type | Bedeutung | Beispiel |
|------------|------|-----------|----------|
| `*` | Greedy | So viele wie möglich | `A*` matcht alle A |
| `*?` | Reluctant | So wenige wie möglich | `A*?` matcht minimal |
| `+` | Greedy | 1 oder mehr (maximal) | `A+` |
| `+?` | Reluctant | 1 oder mehr (minimal) | `A+?` |
| `{n,m}` | Greedy | n bis m (maximal) | `A{2,5}` |
| `{n,m}?` | Reluctant | n bis m (minimal) | `A{2,5}?` |

**⚠️ Flink verbietet Patterns die mit Greedy Quantifier enden!**

---

## Problem: Event-Journey mit begrenztem State

**Ziel:** Tracke die letzten N Events eines Arbeitssuchenden, ohne unbegrenztes State-Wachstum zu verursachen.

**Business-Kontext:** Die BA will Event-Sequenzen analysieren (z.B. letzte 5 Interaktionen vor einem bestimmten Event), aber OHNE State über Jahre zu akkumulieren.

### ❌ Query 1: Initial (Fehler!)

```sql
SELECT *
FROM orders
MATCH_RECOGNIZE (
    PARTITION BY orderid
    ORDER BY order_time
    MEASURES
        COUNT(*) AS journey_length
    AFTER MATCH SKIP PAST LAST ROW
    PATTERN (E+)
    DEFINE
        E AS TRUE
);
```

**🔍 Teste das Query in Control Center!**

**Fehler:**
```
Pattern ends with greedy quantifier forbidden!
```

**Grund:** `E+` ist greedy und am Ende → unbegrenztes State-Wachstum möglich.

---

### ⚠️ Query 2: Funktioniert, aber Probleme

```sql
SELECT *
FROM orders
MATCH_RECOGNIZE (
    PARTITION BY orderid
    ORDER BY order_time
    MEASURES
        COUNT(*) AS journey_length
    AFTER MATCH SKIP PAST LAST ROW
    PATTERN (E+ V)
    DEFINE
        E AS TRUE,
        V AS TRUE
);
```

**🔍 Teste das Query!**

**Problem:** Unbegrenztes State-Wachstum!

Arbeitssuchender könnte **jahrelang** Events haben → alle werden im State gespeichert!

---

### ⚠️ Query 3: Reluctant Quantifier (auch problematisch)

```sql
SELECT *
FROM orders
MATCH_RECOGNIZE (
    PARTITION BY orderid
    ORDER BY order_time
    MEASURES
        COUNT(*) AS journey_length
    AFTER MATCH SKIP PAST LAST ROW
    PATTERN (E+? V)
    DEFINE
        E AS TRUE,
        V AS TRUE
);
```

**Problem:** `E AS TRUE` matcht **ALLES** → unbegrenztes Akkumulieren!

---

## ✅ Hands-on Exercise: State begrenzen!

**Frage:** Wie begrenzt man State-Wachstum ohne Business-Value zu verlieren?

**Überlegung:** Brauchen wir wirklich ALLE Events aus den letzten **Jahren** für Event-Analyse?

**Antwort:** Nein! Die letzten **5 Events** oder innerhalb **30 Tagen** reichen für Analyse-Zwecke!

---

### ✅ Lösung 1: Limit Event-Count

```sql
SELECT *
FROM orders
MATCH_RECOGNIZE (
    PARTITION BY orderid
    ORDER BY order_time
    MEASURES
        COUNT(*) AS journey_length
    AFTER MATCH SKIP PAST LAST ROW
    PATTERN (E{1,5}? V)
    DEFINE
        E AS TRUE,
        V AS TRUE
);
```

**🔍 Teste das Query!**

**Vorteil:** Max. **6 Events** im State (5 E + 1 V) → begrenzt!

**Business Logic:** "Letzte 5-6 Events pro Kunde"

---

### ✅ Lösung 2: Zeitfenster

```sql
SELECT *
FROM orders
MATCH_RECOGNIZE (
    PARTITION BY orderid
    ORDER BY order_time
    MEASURES
        COUNT(*) AS journey_length
    AFTER MATCH SKIP PAST LAST ROW
    PATTERN (E+? V)
    WITHIN INTERVAL '30' DAYS
    DEFINE
        E AS TRUE,
        V AS TRUE
);
```

**🔍 Teste das Query!**

**Vorteil:** Nur Events innerhalb **30 Tage** → begrenzt!

**Business Logic:** "Events innerhalb 30 Tagen"

---

### ✅ Lösung 3: Beides kombinieren (Best Practice!)

```sql
SELECT *
FROM orders
MATCH_RECOGNIZE (
    PARTITION BY orderid
    ORDER BY order_time
    MEASURES
        COUNT(*) AS journey_length
    AFTER MATCH SKIP PAST LAST ROW
    PATTERN (E{1,5}? V)
    WITHIN INTERVAL '30' DAYS
    DEFINE
        E AS TRUE,
        V AS TRUE
);
```

**🔍 Teste das Query!**

**Best Practice:** Max. **6 Events** UND innerhalb **30 Tage**!

**Doppelte Absicherung:** Verhindert State-Explosion!

---

## Test: Events generieren

Order-Daten werden automatisch durch den Datagen Connector generiert.

**Hinweis:** Die Pattern-Erkennung funktioniert mit zufälligen Order-Daten. Im BA-Kontext interpretieren wir:
- `orderid` als Arbeitssuchender-ID
- Jeder Order-Event als Interaktion im System

---

## Erweiterte Use Cases

### Event-Häufigkeit pro Kunde

```sql
SELECT
    customer_id as arbeitssuchender,
    journey_length
FROM orders
MATCH_RECOGNIZE (
    PARTITION BY orderid
    ORDER BY order_time
    MEASURES
        COUNT(*) AS journey_length
    AFTER MATCH SKIP PAST LAST ROW
    PATTERN (E{1,3}? V)
    WITHIN INTERVAL '30' DAYS
    DEFINE
        E AS TRUE,
        V AS TRUE
)
WHERE journey_length >= 2;
```

**BA Use Case:** "Welche Arbeitssuchenden hatten 2-4 Interaktionen innerhalb 30 Tagen?"

---

## ⚠️ CP Flink Einschränkungen

**Was funktioniert:**
- ✅ MATCH_RECOGNIZE Pattern-Syntax
- ✅ MEASURES mit SUM, COUNT, MIN, MAX
- ✅ PATTERN Quantifiers (`{n,m}`, `+`, `?`)
- ✅ WITHIN time constraints
- ✅ PARTITION BY, ORDER BY

**Was NICHT funktioniert:**
- ❌ `ARRAY_AGG` in MEASURES (CP Flink 8.1.0 limitation)
- ❌ `MATCH_ROWTIME` (ohne Parameter)
- ❌ `PARTITION BY` mit Expressions (z.B. `CAST(...)`) - nutze VIEW mit berechneter Spalte

**Workaround:** Nutze `COUNT(*)` für Anzahl der Events statt vollständige Event-Liste.

---

## 🎯 Summary

Nach diesem Exercise verstehst du:

- ✅ Resource-Limits in MATCH_RECOGNIZE
- ✅ Greedy vs. Reluctant Quantifiers
- ✅ `WITHIN` für Zeitfenster
- ✅ Quantifier Upper Limits `{,N}`
- ✅ Real-World Pattern: Event-Journey (BA)
- ✅ State Management Best Practices
- ✅ VIEW-basierte Lösung für PARTITION BY Expressions

---

## 📚 Resources

- [Flink SQL MATCH_RECOGNIZE (Apache Flink Docs)](https://nightlies.apache.org/flink/flink-docs-release-1.19/docs/dev/table/sql/queries/match_recognize/)
- [Pattern Recognition Tutorial](https://developer.confluent.io/courses/flink-sql/match-recognize/)

---

**Weiter zu:** [Exercise 4: Stream Enrichment](MODULE_14_EXERCISE_STREAM_ENRICHMENT.md)
