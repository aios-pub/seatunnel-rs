# Architecture

## Components

- **seatunnel-api**: Field, Row, ColumnType, Source/Sink/Transform traits
- **seatunnel-engine**: Master/Worker, gRPC, checkpoint, state backend
- **seatunnel-connectors**: Kafka, MySQL/TiDB/PostgreSQL CDC, JDBC
- **seatunnel-formats**: 11 data format serializers/deserializers
- **seatunnel-cli**: clap CLI + ratatui TUI
