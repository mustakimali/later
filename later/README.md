# later

A distributed background job manager and runner for Rust. This is currently in PoC stage.

## Set up

<details>
  <summary>Click to expand: One-time setup during application startup!</summary>

### 1. Import `later` and required dependencies

```toml
later = "0.0.3"
serde = "1.0"

```

### 2. Define some types to use as a payload to the background jobs

```rust
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)] // <- Required derives
pub struct SendEmail {
    pub address: String,
    pub body: String,
}

// ... more as required

```

### 3. Generate the stub

```rust
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

```rust
// bootstrap the server
let job_ctx = JobContext {};
let ctx = MyContext{ /*..*/ };                  // Any context to pass onto the handlers
let storage = Redis::new("redis://127.0.0.1/")  // More storage option to be available later
    .await
    .expect("connect to redis");
let ctx = JobsBuilder::new(
        ctx,                                        // Pass the context here
        "later-example".into(),                     // Unique name for this app
                                                    // Ensure the this is same in multiple
                                                    // instances of this app.
        "amqp://guest:guest@localhost:5672".into(), // RabbitMq instance
    )
    // for each payload defined in the `struct Jobs` above
    // the generated fn name uses the pattern "with_[name]_handler"
    .with_send_email_handler(handle_send_email)     // Pass the handler function
    // ..
    .build()
    .expect("start BG Jobs server");

// use ctx.enqueue(SendEmail{ ... }) to enqueue jobs,
// or ctx.enqueue_continue(parent_job_id, SendEmail{ ... }) to chain jobs.
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

</details>

---

## Fire and forget jobs

Fire and forget jobs are executed only once and executed by an available worker almost immediately.

```rust
ctx.enqueue(SendEmail{
    address: "hello@rust-lang.org".to_string(),
    body: "You rock!".to_string() 
});

```

## Continuations

One or many jobs are chained together to create an workflow. Child jobs are executed **only when parent job has been finished**.

```rust
let email_welcome = ctx.enqueue(SendEmail{
    address: "customer@example.com".to_string(),
    body: "Creating your account!".to_string() 
});

let create_account = ctx.enqueue_continue(email_welcome, CreateAccount { ... });

let email_confirmation = ctx.enqueue_continue(create_account, SendEmail{
    address: "customer@example.com".to_string(),
    body: "Your account has been created!".to_string() 
});

```

## Delayed jobs

Just like fire and forget jobs that starts after a certain interval.

```rust
// delay
ctx.enqueue_delayed(SendEmail{
    address: "hello@rust-lang.org".to_string(),
    body: "You rock!".to_string() 
}, std::time::Duration::from_secs(60));

// specific time
let run_job_at : chrono::DateTime<Utc> = ...;
ctx.enqueue_delayed_at(SendEmail{
    address: "hello@rust-lang.org".to_string(),
    body: "You rock!".to_string() 
}, run_job_at);

```

## Recurring jobs

_(Coming soon)_


# Project status

This is PoC at this moment. Upcoming features are

- [ ] Multiple storage backend (redis, postgres)
- [x] Continuation
- [x] Delayed Jobs
- [ ] Recurring jobs
- [ ] Dashboard
- [ ] Use storage backend for scheduling (to remove dependency to RabbitMQ)
- [ ] Ergonomic API