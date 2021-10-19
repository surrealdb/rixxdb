# RixxDB

RixxDB is a versioned, embedded, strongly-consistent, key-value database.

[![](https://img.shields.io/badge/status-alpha-ff00bb.svg?style=flat-square)](https://github.com/surrealdb/rixxdb) [![](https://img.shields.io/badge/godoc-reference-blue.svg?style=flat-square)](https://godoc.org/github.com/surrealdb/rixxdb) [![](https://goreportcard.com/badge/github.com/surrealdb/rixxdb?style=flat-square)](https://goreportcard.com/report/github.com/surrealdb/rixxdb) [![](https://img.shields.io/badge/license-Apache_License_2.0-00bfff.svg?style=flat-square)](https://github.com/surrealdb/rixxdb) 

#### Features

- In-memory database
- Built-in encryption
- Built-in compression
- Built-in item versioning
- Multi-version concurrency control
- Rich transaction support with rollbacks
- Multiple concurrent readers without locking
- Atomicity, Consistency and Isolation from ACID
- Durable, configurable append-only file format for data persistence
- Flexible iteration of data; ascending, descending, ranges, and hierarchical ranges

#### Installation

```bash
go get github.com/surrealdb/rixxdb
```
