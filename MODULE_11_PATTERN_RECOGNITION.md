# Modul 11: Pattern Recognition mit MATCH_RECOGNIZE

**Original:** https://developer.confluent.io/courses/flink-sql/match-recognize/  
**Typ:** Video + Reading  
**Dauer:** 19 min  
**Instruktor:** David Anderson, Principal Software Practice Lead

---

## 🎥 Video

**YouTube:** https://www.youtube.com/watch?v=5TK8UrmU83I

---

## Überblick

`MATCH_RECOGNIZE` ist eine SQL-Erweiterung für **Pattern Matching** in Event-Streams. Erkenne komplexe Event-Sequenzen wie "3+ fehlgeschlagene Logins in 5 Minuten" oder "Customer Journey: Browse → Cart → Purchase".

Hauptkomponenten:
- **PATTERN** - Definiert Event-Sequenz (mit Quantifiern: `{3,}`, `+`, `?`)
- **DEFINE** - Definiert Pattern-Variablen
- **MEASURES** - Definiert Output
- **WITHIN** - Zeitliche Begrenzung (wichtig für State Management!)

---

## Ressourcen

**Dokumentation:**
- [MATCH_RECOGNIZE (Apache Flink Docs)](https://nightlies.apache.org/flink/flink-docs-release-1.19/docs/dev/table/sql/queries/match_recognize/)


---

**Weiter zu:** [Modul 12: Exercise - MATCH_RECOGNIZE](MODULE_12_EXERCISE_MATCH_RECOGNIZE.md)
