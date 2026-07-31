# Modul 10: The Flink SQL Runtime

**Original:** https://developer.confluent.io/courses/flink-sql/flink-sql-runtime/  
**Typ:** Video + Reading  
**Dauer:** 10 min  
**Instruktor:** David Anderson, Principal Software Practice Lead

---

## 🎥 Video

**YouTube:** (URL wird ergänzt sobald verfügbar)

---

## Überblick

Flink SQL ist darauf ausgelegt, **State zu minimieren** durch temporale Operationen. Wichtige Konzepte:

- **Stateless vs Stateful Queries** - Filter vs Aggregationen
- **Insert-Only vs Updating Streams** - Append-only ist einfacher zu verarbeiten
- **Watermarks** - Notwendig für Komposition mit temporalen Operationen
- **Checkpointing** - Garantiert Exactly-Once Semantics

**Key Insight:** Streaming = Batch (gleiche Ergebnisse, unterschiedliche Ausführung)

---

## Ressourcen

**Dokumentation:**
- [Flink Architecture](https://nightlies.apache.org/flink/flink-docs-release-1.19/docs/concepts/flink-architecture/)
- [Checkpointing](https://nightlies.apache.org/flink/flink-docs-release-1.19/docs/dev/datastream/fault-tolerance/checkpointing/)


---

**Weiter zu:** [Modul 11: Pattern Recognition](MODULE_11_PATTERN_RECOGNITION.md)
