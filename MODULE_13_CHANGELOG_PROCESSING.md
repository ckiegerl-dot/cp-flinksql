# Modul 13: Changelog Processing

**Original:** https://developer.confluent.io/courses/flink-sql/changelog-processing/  
**Typ:** Video + Reading  
**Dauer:** 10 min  
**Instruktor:** David Anderson, Principal Software Practice Lead

---

## 🎥 Video

**YouTube:** https://www.youtube.com/watch?v=nlW0INe09U8

---

## Überblick

Changelog Processing verarbeitet **Updates und Deletes** in Streaming-Daten. Flink SQL unterstützt drei Modi:

- **append** - Nur `+I` (Insert)
- **retract** - `+I`, `-U`, `+U`, `-D` (vollständiger Changelog)
- **upsert** - `+I`, `+U` mit Primary Key (effizienter)

**Stream-Table Duality:** Jeder Stream kann als Table gesehen werden (und umgekehrt).

**Best Practice:** Append-Only bevorzugen, Windows und Temporal JOINs verwenden, State TTL setzen.

---

## Ressourcen

**Dokumentation:**
- [Dynamic Tables](https://nightlies.apache.org/flink/flink-docs-release-1.19/docs/dev/table/concepts/dynamic_tables/)
- [Upsert Kafka Connector](https://nightlies.apache.org/flink/flink-docs-release-1.19/docs/connectors/table/upsert-kafka/)


---

**Weiter zu:** [Modul 14: Exercise - Stream Enrichment](MODULE_14_EXERCISE_STREAM_ENRICHMENT.md)
