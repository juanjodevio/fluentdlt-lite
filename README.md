## 🧠 Philosophy

**FluentDLT** (`fldt`) is a fluent interface wrapper for [dlt (data load tool)](https://dlthub.com) that makes building data pipelines intuitive and readable. Write pipelines that read like English, with full type safety and comprehensive error handling.

```python
from fldt import FluentPipeline

# Load from SQL, transform, and send to data warehouse
result = (FluentPipeline
    .from_sql_table("postgresql://user:pass@localhost/db", "users")
    .add_transformer(clean_data)
    .with_incremental("updated_at")
    .to("duckdb")
    .run())
```

### Core Principles

- **🔄 Fluent** — Pipelines read like sentences with method chaining
- **⚡ Simple** — Minimal boilerplate, maximum clarity
- **🛡️ Type-Safe** — Full type hints and validation
- **🎯 Explicit** — No magic, clear error messages
- **🚀 Powered by DLT** — Reliable, scalable pipelines under the hood

---