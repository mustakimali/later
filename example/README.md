# later examples

There are various examples.
* postgres - Using Postgres as storage backend
* redis - Using Postgres as storage backend

```bash
export ENABLE_JAEGER=true # optional, to enable traces to jaeger UI

cargo run --bin postgres
```

## Dependencies

Run all dependencies using the `scripts/init-test.sh` script.

View the [jaeger UI](http://localhost:16686)