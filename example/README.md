# later examples

There are various examples.
* postgres - Using Postgres as storage backend
* redis - Using Postgres as storage backend and enables the experimental dashboard feature.

```bash
export ENABLE_JAEGER=true # optional, to enable traces to jaeger UI

cargo run --bin redis
```

* Enqueue a simple job [`http://localhost:8000/enqueue/1?delay_sec=1`](http://localhost:8000/enqueue/1?delay_sec=1)
* View dashboard at [`http://localhost:8000/dash`](http://localhost:8000/dash)
* View metrics at [`http://localhost:8000/metrics`](http://localhost:8000/metrics)

## Dependencies

Run all dependencies using the `scripts/init-test.sh` script.

View the [jaeger UI](http://localhost:16686)