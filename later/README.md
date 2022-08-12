# later

A distributed background job manager and runner for Rust. This is currently in PoC stage.

## Set up

Look at the [documentations](https://docs.rs/later/latest/later/#later) for details. In general the one time setup involves:

* Import `later` and required dependencies
* Define some types to use as a payload to the background jobs
* Generate the stub
* Use the generated code to bootstrap the background job server

## Features

### Fire and forget jobs

Fire and forget jobs are executed only once and executed by an available worker almost immediately.

### Continuations

One or many jobs are chained together to create an workflow. Child jobs are executed **only when parent job has been finished**.

### Delayed jobs

Just like fire and forget jobs that starts after a certain interval.

### Recurring jobs

(_wip_)

Run recurring jobs based on cron schedule. 
* To fix: to delete recurring job.


## Project status

This is PoC at this moment. Upcoming features are

- [x] Multiple storage backend (redis, postgres)
- [x] Continuation
- [x] Delayed Jobs
- [x] Recurring jobs
- [ ] Dashboard
- [ ] Use storage backend for scheduling (to remove dependency to RabbitMQ)
- [x] Ergonomic API