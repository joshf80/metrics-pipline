 ## Real Time Metrics Pipeline

A low latency, time-series ingestion pipeline with a real time dashboard. Built in C++23 with modern design patterns. Handles high volume event streams, normalizes them into a common schema, and exposes them for query and visualization.

## Why

Most time series tools are expensive or complex to self host, this single binary streams, stores and visualizes data with minimal setup.

Architecture is source agnostic. New data source simply requires implementing single interface.

##  Current to do list:

| Technology    | Purpose       |
| ------------- |:-------------:|     
| SQLite (?) | simple storage  |
| Prometheus | metrics collection |
| Grafana | visualization |
| C++23 | dashboard backend |
| Thread Pool| request handling |
| Ring Buffer | Ingestion decoupling |
| Docker | deployment |