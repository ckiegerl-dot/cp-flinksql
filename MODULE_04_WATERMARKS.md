# Modul 4: Wie Streaming SQL Watermarks nutzt

**Original:** https://developer.confluent.io/courses/flink-sql/watermarks/  
**Typ:** Video + Reading  
**Dauer:** 10 min  
**Instruktor:** David Anderson, Principal Software Practice Lead

---

## 🎥 Video

**YouTube:** https://www.youtube.com/watch?v=2R6ZGh6nNiA

---

## Überblick

Watermarks sind **spezielle Records**, die in Data Streams eingefügt werden, um **den Fortschritt der Zeit** zu markieren. Sie sind notwendig für zeitbasierte Operationen wie Window Aggregations, Joins und Sorting.

Flink nutzt **Bounded Out-of-Orderness**: `Watermark = max(event_timestamp) - tolerance`

---

## Ressourcen

**Dokumentation:**
- [Time and Watermarks (Confluent Docs)](https://docs.confluent.io/cloud/current/flink/concepts/timely-stream-processing.html)
- [Event Time and Watermarks (Apache Flink Docs)](https://nightlies.apache.org/flink/flink-docs-release-1.19/docs/dev/datastream/event-time/generating_watermarks/)


---

**Weiter zu:** [Modul 5: Exercise - Hands-on with Watermarks](MODULE_05_EXERCISE_WATERMARKS.md)
