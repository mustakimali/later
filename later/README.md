# later

A distributed background job manager and runner for Rust. This is currently in PoC stage.

## How to enqueue Fire and Forget jobs

### 1. Import `later` and required dependencies

```toml
later = "0.0.3"
serde = "1.0"

```

### 2. Define some types to use as a payload to the background jobs

```rs
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)] // <- Required derives
pub struct SendEmail {
    pub address: String,
    pub body: String,
}

// ... more as required

```

### 3. Generate the stub

```rs
later::background_job! {
    struct Jobs {
        // Use the format
        // name: Payload type (defined above)

        send_email: SendEmail,
        // ..
    }
}
```

This generates two types
* `JobsBuilder` - used to bootstrap the background job server - which can be used to enqueue jobs,
* `JobContext<T>` - used to pass application context (`T`) in the handler as well as enqueue jobs,

### 4. Use the generated code to bootstrap the background job server

For `struct Jobs` a type `JobsBuilder` will be generated. Use this to bootstrap the server.

```rs
// bootstrap the server
let job_ctx = JobContext {};
let ctx = MyContext{ /*..*/ };                  // Any context to pass onto the handlers
let storage = Redis::new("redis://127.0.0.1/")  // More storage option to be available later
    .await
    .expect("connect to redis");
let bg_jobs = JobsBuilder::new(
        ctx,                                        // Pass the context here
        "later-example".into(),                     // Unique name for this app
        "amqp://guest:guest@localhost:5672".into(), // RabbitMq instance
    )
    // for each payload defined in the `struct Jobs` above
    // the generated fn name uses the pattern "with_[name]_handler"
    .with_send_email_handler(handle_send_email)     // Pass the handler function
    // ..
    .build()
    .expect("start BG Jobs server");

// use bg_jobs.enqueue(SendEmail{ ... }) to enqueue jobs,
// or bg_jobs.enqueue_continue(parent_job_id, SendEmail{ ... }) to chain jobs.
// this will only accept types defined inside the macro above

// define handler
fn handle_send_email(
        ctx: &JobContext<MyContext>, // JobContext is generated wrapper
        payload: SendEmail,
    ) -> anyhow::Result<()> {
        // handle `payload`

        // ctx.app -> Access the MyContext passed during bootstrapping
        // ctx.enqueue(_) to enqueue more jobs
        // ctx.enqueue_continue(_) to chain jobs

        Ok(()) // or Err(_) to retry this message
    }
```


# Project status

This is PoC at this moment. I aim to make something like [Hangfire for .NET](https://www.hangfire.io/).
Upcoming features are

- [ ] Multiple storage backend (redis, postgres)
- [x] Continuation
- [ ] Delayed Jobs (WIP)
- [ ] Recurring jobs
- [ ] Dashboard
- [ ] Use storage backend for scheduling (to remove dependency to RabbitMQ)