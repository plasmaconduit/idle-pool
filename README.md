# Idle-Pool

Idle-Pool is an implementation of a thread pool that is meant to dequeue
connections directly from a connection source (tcp or unix domain sockets).

This solves the double queue problem when you have an acceptor thread pulling
connections from a listening socket that is backed by a connection queue in
the kernel, only to requeue them in an in-process queue to get picked up by
a worker thread.

With Idle-Pool, the worker threads pull connections directly from the kernel
and back pressure builds up against the listening socket's in kernel tcp
backlog in the presence of congestion.

## Example

Multithreaded tcp echo server:
```rust
use idle_pool::{IdlePoolConfig, TcpStreamIdlePoolWorkProducer};
use std::io::{Result, Read, Write};
use std::mem::MaybeUninit;

fn main() -> Result<()> {
    let min_idle = 100;
    let max_idle = 300;
    let max_total = 5000;
    let idle_pool_config = IdlePoolConfig::new("echo", min_idle, max_idle, max_total);
    let idle_pool_producer = TcpStreamIdlePoolWorkProducer::bind("0.0.0.0:1337")?;
    let idle_pool = idle_pool_config.spawn(idle_pool_producer, |(mut stream, _)| loop {
        let mut buffer: [u8; 8192] = unsafe { MaybeUninit::uninit().assume_init() };
        let bytes_read = stream.read(&mut buffer)?;
        let response = &buffer[..bytes_read];
        stream.write_all(response)?;
    });
    idle_pool.join();
    Result::Ok(())
}
```

You can also use unix domain sockets. Just switch out 
`TcpStreamIdlePoolWorkProducer` for `UdsStreamIdlePoolWorkProducer`.
```rust
let idle_pool_producer = UdsStreamIdlePoolWorkProducer::bind("/var/run/echo.sock")?;
```