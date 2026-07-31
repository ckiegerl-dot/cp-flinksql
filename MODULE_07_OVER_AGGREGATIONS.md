# Modul 7: OVER Aggregations

**Original:** https://developer.confluent.io/courses/flink-sql/over-aggregations/  
**Typ:** Video + Reading  
**Dauer:** 5 min  
**Instruktor:** David Anderson, Principal Software Practice Lead

---

## 🎥 Video

**YouTube:** (URL wird ergänzt sobald verfügbar)

---

## Überblick

OVER Aggregations (Window Functions) erlauben **Berechnungen über geordnete Zeilen** ohne die Anzahl der Zeilen zu reduzieren. Im Gegensatz zu GROUP BY bleibt **jede Zeile erhalten**.

Wichtige Funktionen:
- **ROW_NUMBER, RANK, DENSE_RANK** - Ranking
- **LAG, LEAD** - Zugriff auf vorherige/nächste Zeilen
- **SUM, AVG, COUNT** - Aggregate mit OVER Clause

**Wichtig:** `ORDER BY` muss auf Time Attribute basieren!

---

## Ressourcen

**Dokumentation:**
- [OVER Aggregation (Apache Flink Docs)](https://nightlies.apache.org/flink/flink-docs-release-1.19/docs/dev/table/sql/queries/over-agg/)


---

**Weiter zu:** [Modul 8: Exercise - Streaming Analytics](MODULE_08_EXERCISE_STREAMING_ANALYTICS.md)
