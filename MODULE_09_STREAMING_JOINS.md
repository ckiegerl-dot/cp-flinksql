# Modul 9: Streaming JOINs

**Original:** https://developer.confluent.io/courses/flink-sql/streaming-joins/  
**Typ:** Video + Reading  
**Dauer:** 20 min  
**Instruktor:** David Anderson, Principal Software Practice Lead

---

## 🎥 Video

**YouTube:** https://www.youtube.com/watch?v=ChiAXgTuzaA

---

## Überblick

JOINs in Streaming funktionieren anders als in Datenbanken, weil Streams **nie enden**. Flink SQL bietet drei Haupttypen:

- **Interval JOIN** - Zeitbasierte Begrenzung (z.B. Orders ↔ Shipments innerhalb 24h)
- **Temporal JOIN** - Join mit Versioned Table (`FOR SYSTEM_TIME AS OF`)
- **Lookup JOIN** - On-demand Zugriff auf externe Datenbank

**Wichtig:** Regular JOINs vermeiden (unbegrenztes State Growth)!

---

## Ressourcen

**Dokumentation:**
- [Joins (Apache Flink Docs)](https://nightlies.apache.org/flink/flink-docs-release-1.19/docs/dev/table/sql/queries/joins/)
- [Temporal Joins](https://nightlies.apache.org/flink/flink-docs-release-1.19/docs/dev/table/sql/queries/joins/#temporal-joins)


---

**Weiter zu:** [Modul 10: The Flink SQL Runtime](MODULE_10_FLINK_SQL_RUNTIME.md)
