# Modul 1: Apache Flink® SQL - Kursübersicht

**Original:** https://developer.confluent.io/courses/flink-sql/overview/  
**Typ:** Reading  
**Instruktor:** David Anderson, Principal Software Practice Lead

**Demo Environment:** https://github.com/ethaden/confluent-platform-flink  
Diese Version basiert auf Eikes Confluent Platform Demo mit Flink auf Kubernetes (KIND)

---

## Was du in diesem Kurs lernst

Dieser Kurs umfasst Videos, Readings und praktische Exercises. Themen:

- **Getting Started** mit CP Flink (Exercise)
- **Watermarks** und Event-Time Processing
- **Window Aggregations** (TUMBLE, HOP, CUMULATE, SESSION)
- **OVER Aggregations** (LAG, LEAD, ROW_NUMBER)
- **Streaming Analytics** (Exercise)
- **Streaming JOINs** (verschiedene Join-Typen)
- **Flink SQL Runtime** (wie Flink SQL funktioniert)
- **Pattern Recognition** mit MATCH_RECOGNIZE
- **MATCH_RECOGNIZE** (Exercise)
- **Changelog Processing** (Update-Verarbeitung)
- **Stream Enrichment** (Exercise)
- **Using EXPLAIN** für Troubleshooting

---

## Zielgruppe

Dieser Kurs richtet sich an alle, die Apache Flink und Flink SQL lernen wollen.

**Keine Vorkenntnisse erforderlich:**
- Du musst nichts über Apache Flink wissen
- SQL-Kenntnisse sind hilfreich, aber nicht zwingend erforderlich
- Die praktischen Exercises setzen voraus, dass du Zugriff auf ein System hast, auf dem du die Confluent CLI installieren kannst

---

## Dauer

**Gesamt:** Ca. 4.5 Stunden (inkl. Videos, Readings und Exercises)

**Aufschlüsselung:**
- Theorie-Module (Videos + Readings): ~2 Stunden
- Praktische Exercises: ~2.5 Stunden

---

## Instruktor

### David Anderson - Course Author

David arbeitet seit vielen Jahren als Data Engineer - noch bevor dieser Jobtitel erfunden wurde. Er hat an Recommender Systems, Search Engines, Machine Learning Pipelines und BI Tools gearbeitet, und hilft Unternehmen seit 2016 bei der Einführung von Stream Processing und Apache Flink.

David ist ein **Apache Flink Committer** und arbeitet bei Confluent als **Principal Software Practice Lead**.

**LinkedIn:** [David Anderson](https://www.linkedin.com/in/david-g-anderson/)

---

## Was ist Flink SQL?

Flink SQL ist eine **SQL Engine** für die Verarbeitung von Batch- und Streaming-Daten mit:
- **Skalierbarkeit** von Apache Flink
- **Performance** für große Datenmengen
- **Konsistenz** (Exactly-Once Semantics)

**Wichtig:** Flink SQL ist **ANSI SQL konform** - du kannst Standard-SQL verwenden!

---

## Warum Flink SQL?

Dieser Kurs nutzt Flink SQL, weil:

1. **SQL ist bekannt:** Du kannst dein bestehendes SQL-Wissen nutzen
2. **Fokus auf Konzepte:** Wir konzentrieren uns auf die **großen Ideen** in Flink, nicht auf Sprach-Details
3. **Einfacher Einstieg:** Flink SQL ist die **einfachste Art** mit Apache Flink zu starten
4. **Batch & Streaming:** Eine Sprache für beide Modi

---

## Diese Anpassung

Dieser Kurs wurde für **CP Flink (Confluent Platform Flink)** angepasst:

- Nutzt **Confluent Platform 8.1.0**
- Läuft auf **Kubernetes** (Eikes Demo Environment)
- Zugriff via **Control Center** (http://localhost:9021)
- Alle SQL-Befehle für **CP Flink** angepasst

---

## Kurs-Module (15 Total)

| # | Modul | Typ | Datei |
|---|-------|-----|-------|
| 1 | Apache Flink SQL Overview | 📖 Reading | *Diese Datei* |
| 2 | What is Flink SQL? | 📖 + 🎥 Video | [MODULE_02](MODULE_02_WHAT_IS_FLINK_SQL.md) |
| 3 | **Get Started** | ✏️ Exercise | [MODULE_03](MODULE_03_EXERCISE_GET_STARTED.md) |
| 4 | How Streaming SQL Uses Watermarks | 📖 + 🎥 Video | [MODULE_04](MODULE_04_WATERMARKS.md) |
| 5 | **Hands-on with Watermarks** | ✏️ Exercise | [MODULE_05](MODULE_05_EXERCISE_WATERMARKS.md) |
| 6 | Window Aggregations | 📖 + 🎥 Video | [MODULE_06](MODULE_06_WINDOW_AGGREGATIONS.md) |
| 7 | OVER Aggregations | 📖 + 🎥 Video | [MODULE_07](MODULE_07_OVER_AGGREGATIONS.md) |
| 8 | **Streaming Analytics** | ✏️ Exercise | [MODULE_08](MODULE_08_EXERCISE_STREAMING_ANALYTICS.md) |
| 9 | Streaming JOINs | 📖 + 🎥 Video | [MODULE_09](MODULE_09_STREAMING_JOINS.md) |
| 10 | The Flink SQL Runtime | 📖 + 🎥 Video | [MODULE_10](MODULE_10_FLINK_SQL_RUNTIME.md) |
| 11 | Pattern Recognition with MATCH_RECOGNIZE | 📖 + 🎥 Video | [MODULE_11](MODULE_11_PATTERN_RECOGNITION.md) |
| 12 | **Match Recognize Exercise** | ✏️ Exercise | [MODULE_12](MODULE_12_EXERCISE_MATCH_RECOGNIZE.md) |
| 13 | Changelog Processing | 📖 + 🎥 Video | [MODULE_13](MODULE_13_CHANGELOG_PROCESSING.md) |
| 14 | **Stream Enrichment** | ✏️ Exercise | [MODULE_14](MODULE_14_EXERCISE_STREAM_ENRICHMENT.md) |
| 15 | Using EXPLAIN | 📖 + 🎥 Video | [MODULE_15](MODULE_15_USING_EXPLAIN.md) |

---

## Wie du den Kurs nutzt

### 1. Setup (einmalig)

Stelle sicher dass die Demo-Umgebung läuft:

```bash
./start.sh
```

### 2. Reihenfolge

Arbeite die Module **in Reihenfolge** durch:
1. Lies zuerst die Theorie (MODULE_XX.md mit Video)
2. Mache dann das Exercise (EXERCISE_XX.md)

### 3. Exercises

Alle praktischen Exercises haben:
- ✅ **Setup-Anleitung** (Topics erstellen, Daten generieren)
- ✅ **SQL Queries** (copy-paste ready!)
- ✅ **Erwartete Ergebnisse**
- ✅ **Cleanup** (am Ende aufräumen)

---

## Support & Community

**Fragen oder Kommentare?**

Tritt dem **Confluent Community Slack** bei: https://cnfl.io/slack-cd

Channel: `#developer-confluent-io`

---

## Los geht's!

Starte mit [Modul 2: What is Flink SQL?](MODULE_02_WHAT_IS_FLINK_SQL.md)

Oder springe direkt zum ersten Exercise: [Modul 3: Get Started](MODULE_03_EXERCISE_GET_STARTED.md)
