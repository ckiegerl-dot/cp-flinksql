# Modul 15: Using EXPLAIN für Troubleshooting

**Original:** https://developer.confluent.io/courses/flink-sql/using-explain-for-troubleshooting/  
**Typ:** Video + Reading  
**Dauer:** 19 min  
**Instruktor:** David Anderson, Principal Software Practice Lead

---

## 🎥 Video

**YouTube:** https://www.youtube.com/watch?v=LOmHQvYFQdw

---

## Überblick

EXPLAIN zeigt den **Execution Plan** für Flink SQL Statements. Der Output macht klar, welche Operatoren verwendet werden und wie viel State sie benötigen.

Wichtige Konzepte:
- **StreamCalc** - Stateless Operations (Filter, Projections)
- **StreamExchange** - Netzwerk-Shuffle (teuer!)
- **State Kategorien** - Stateless, Constant, Per-Key, Per-Row
- **Changelog Normalization** - Konvertiert Upsert → Retract
- **Upsert Materialization** - Konvertiert Retract → Upsert (State-intensiv!)

**Best Practice:** EXPLAIN für komplette INSERT Statements nutzen, State TTL setzen, Append-Only bevorzugen.

---

## Ressourcen

**Dokumentation:**
- [EXPLAIN Statement (Confluent Docs)](https://docs.confluent.io/cloud/current/flink/reference/statements/explain.html)
- [State TTL](https://nightlies.apache.org/flink/flink-docs-release-1.19/docs/dev/table/concepts/overview/#idle-state-retention-time)


---

## Abschluss

**Herzlichen Glückwunsch! 🎉**

Du hast alle 15 Module des Apache Flink® SQL Kurses abgeschlossen!

**Nächste Schritte:**
- Baue eigene Streaming Pipelines
- Experimentiere mit den Exercises
- Nutze EXPLAIN für deine Production Queries

**Viel Erfolg mit Apache Flink! 🚀**
