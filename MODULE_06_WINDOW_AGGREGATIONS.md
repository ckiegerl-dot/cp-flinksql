# Modul 6: Window Aggregations

**Original:** https://developer.confluent.io/courses/flink-sql/window-aggregations/  
**Typ:** Video + Reading  
**Dauer:** 6 min  
**Instruktor:** David Anderson, Principal Software Practice Lead

---

## 🎥 Video

**YouTube:** https://www.youtube.com/watch?v=Xgg9IndH8LQ

---

## Überblick

Window Aggregations gruppieren Events in **zeitliche Fenster**. Flink SQL bietet vier Window Table-Valued Functions (TVFs):

- **TUMBLE** - Nicht-überlappende, feste Intervalle
- **HOP** - Überlappende, gleitende Fenster
- **CUMULATE** - Wachsende Fenster innerhalb eines Zeitraums
- **SESSION** - Dynamische Fenster basierend auf Inaktivität

**Wichtig:** Immer `GROUP BY window_start, window_end` verwenden!

---

## Ressourcen

**Dokumentation:**
- [Window Aggregations (Confluent Docs)](https://docs.confluent.io/cloud/current/flink/reference/queries/window-tvf.html)
- [Window Table-Valued Functions (Apache Flink Docs)](https://nightlies.apache.org/flink/flink-docs-release-1.19/docs/dev/table/sql/queries/window-tvf/)


---

**Weiter zu:** [Modul 7: OVER Aggregations](MODULE_07_OVER_AGGREGATIONS.md)
