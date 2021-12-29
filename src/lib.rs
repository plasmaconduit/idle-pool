use log::trace;
use std::io::ErrorKind::Other;
use std::io::{Error, Result};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::os::unix::io::AsRawFd;
use std::os::unix::net::{UnixListener, UnixStream};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::thread::JoinHandle;
use std::{ptr, thread};

use std::os::unix::thread::JoinHandleExt;

pub trait IdlePoolWorkProducer {
    type Out;
    fn next(&self) -> Result<Self::Out>;
    fn end(&self);
}

pub struct TcpStreamIdlePoolWorkProducer {
    listener: TcpListener,
}

impl TcpStreamIdlePoolWorkProducer {
    pub fn new(listener: TcpListener) -> TcpStreamIdlePoolWorkProducer {
        TcpStreamIdlePoolWorkProducer { listener }
    }

    pub fn bind(address: &str) -> Result<TcpStreamIdlePoolWorkProducer> {
        TcpListener::bind(address).map(Self::new)
    }
}

impl IdlePoolWorkProducer for TcpStreamIdlePoolWorkProducer {
    type Out = (TcpStream, SocketAddr);

    fn next(&self) -> Result<(TcpStream, SocketAddr)> {
        self.listener.accept()
    }

    fn end(&self) {
        unsafe {
            libc::close(self.listener.as_raw_fd());
        }
    }
}

pub struct UdsStreamIdlePoolWorkProducer {
    listener: UnixListener,
}

impl UdsStreamIdlePoolWorkProducer {
    pub fn new(listener: UnixListener) -> UdsStreamIdlePoolWorkProducer {
        UdsStreamIdlePoolWorkProducer { listener }
    }

    pub fn bind(address: &str) -> Result<UdsStreamIdlePoolWorkProducer> {
        UnixListener::bind(address).map(Self::new)
    }
}

impl IdlePoolWorkProducer for UdsStreamIdlePoolWorkProducer {
    type Out = (UnixStream, std::os::unix::net::SocketAddr);

    fn next(&self) -> Result<(UnixStream, std::os::unix::net::SocketAddr)> {
        self.listener.accept()
    }

    fn end(&self) {
        unsafe {
            libc::close(self.listener.as_raw_fd());
        }
    }
}

fn manager_thread<P, F>(config: IdlePoolConfig, state: Arc<IdlePoolState<P, F>>)
where
    P: IdlePoolWorkProducer + Send + Sync + 'static,
    F: Fn(P::Out) -> Result<()> + Sync + Send + 'static,
{
    loop {
        {
            let threads_to_spawn = state.calculate_threads_to_spawn(&config);
            trace!(target: "idle_queue_events", "pool={} spawning={}",
                config.name,
                threads_to_spawn);
            for _ in 0..threads_to_spawn {
                let cloned_state = state.clone();
                let cloned_config = config.clone();
                thread::spawn(move || worker_thread(cloned_config, cloned_state));
            }
        }
        trace!(target: "idle_queue_events", "pool={} waiting for idle thread deficit", config.name);
        state.wait_for_idle_deficit(&config);
        if !state.is_running() {
            trace!(target: "idle_queue_events", "pool={} waiting for drain", config.name);
            state.wait_for_drain();
            trace!(target: "idle_queue_events", "pool={} shutdown", config.name);
            break;
        }
    }
}

fn worker_thread<P, F>(config: IdlePoolConfig, state: Arc<IdlePoolState<P, F>>) -> Result<()>
where
    P: IdlePoolWorkProducer + Send + Sync + 'static,
    F: Fn(P::Out) -> Result<()> + Sync + Send + 'static,
{
    return state.observe_worker(&config, |producer, operation| loop {
        let out = producer.next()?;
        state.observe_busy_worker(&config, operation, out)?;
    });
}

struct IdlePoolCounters {
    total: usize,
    busy: usize,
}

impl IdlePoolCounters {
    pub fn new() -> IdlePoolCounters {
        IdlePoolCounters { total: 0, busy: 0 }
    }

    pub fn idle(&self) -> usize {
        self.total - self.busy
    }

    pub fn increase_total(&mut self) {
        self.total += 1
    }

    pub fn decrease_total(&mut self) {
        self.total -= 1
    }

    pub fn increase_busy(&mut self) {
        self.busy += 1
    }

    pub fn decrease_busy(&mut self) {
        self.busy -= 1
    }
}

struct IdlePoolState<P, F>
where
    P: IdlePoolWorkProducer + Send + Sync + 'static,
    F: Fn(P::Out) -> Result<()> + Sync + Send + 'static,
{
    mutex_counters: Mutex<IdlePoolCounters>,
    cvar: Condvar,
    atomic_running: AtomicBool,
    producer: P,
    operation: F,
}

impl<P, F> IdlePoolState<P, F>
where
    P: IdlePoolWorkProducer + Send + Sync + 'static,
    F: Fn(P::Out) -> Result<()> + Sync + Send + 'static,
{
    fn new(producer: P, operation: F) -> Arc<IdlePoolState<P, F>> {
        Arc::new(IdlePoolState {
            mutex_counters: Mutex::new(IdlePoolCounters::new()),
            cvar: Condvar::new(),
            atomic_running: AtomicBool::new(true),
            producer,
            operation,
        })
    }

    fn notify_idle_pool_manager(&self) {
        self.cvar.notify_one();
    }

    fn shutdown(&self) {
        self.atomic_running.store(false, Ordering::Relaxed);
        self.producer.end();
        self.cvar.notify_one();
    }

    fn is_running(&self) -> bool {
        self.atomic_running.load(Ordering::Relaxed)
    }

    fn observe_worker<FW>(&self, config: &IdlePoolConfig, worker: FW) -> Result<()>
    where
        FW: Fn(&P, &F) -> Result<()>,
    {
        {
            let mut counters = self.mutex_counters.lock().unwrap();
            if counters.total >= config.max_total {
                self.notify_idle_pool_manager();
                return Result::Err(Error::from(Other));
            }
            counters.increase_total();
            if counters.idle() > config.max_idle {
                counters.decrease_total();
                self.notify_idle_pool_manager();
                return Result::Err(Error::from(Other));
            }
            self.notify_idle_pool_manager();
        }
        let result = worker(&self.producer, &self.operation);
        {
            let mut counters = self.mutex_counters.lock().unwrap();
            counters.decrease_total();
            self.notify_idle_pool_manager();
        }
        result
    }

    fn observe_busy_worker(
        &self,
        config: &IdlePoolConfig,
        operation: &F,
        out: P::Out,
    ) -> Result<()> {
        {
            let mut counters = self.mutex_counters.lock().unwrap();
            counters.increase_busy();
            if counters.idle() < config.min_idle {
                self.notify_idle_pool_manager();
            }
        }
        let result = operation(out);
        {
            let mut counters = self.mutex_counters.lock().unwrap();
            counters.decrease_busy();
            let idle_now = counters.idle();
            if idle_now > config.max_idle {
                return Result::Err(Error::from(Other));
            }
        }
        result
    }

    fn calculate_threads_to_spawn(&self, config: &IdlePoolConfig) -> usize {
        let counters = self.mutex_counters.lock().unwrap();
        let idle = counters.idle();
        return if idle < config.min_idle {
            let min_difference = config.min_idle - idle;
            let uncapped_total = counters.total + min_difference;
            if uncapped_total > config.max_total {
                config.max_total - counters.total
            } else {
                min_difference
            }
        } else {
            0
        };
    }

    fn wait_for_idle_deficit(&self, config: &IdlePoolConfig) {
        let mut counters = self.mutex_counters.lock().unwrap();
        trace!(target: "idle_queue_events", "pool={} total={} busy={}", config.name, counters.total, counters.busy);
        while (counters.idle() >= config.min_idle || counters.total >= config.max_total)
            && self.is_running()
        {
            counters = self.cvar.wait(counters).unwrap();
            trace!(target: "idle_queue_events", "pool={} total={} busy={}", config.name, counters.total, counters.busy);
        }
    }

    fn wait_for_drain(&self) {
        let mut counters = self.mutex_counters.lock().unwrap();
        while counters.total > 0 {
            counters = self.cvar.wait(counters).unwrap();
        }
    }
}

#[derive(Clone)]
pub struct IdlePoolConfig {
    name: &'static str,
    min_idle: usize,
    max_idle: usize,
    max_total: usize,
}

impl IdlePoolConfig {
    pub fn new(
        name: &'static str,
        min_idle: usize,
        max_idle: usize,
        max_total: usize,
    ) -> IdlePoolConfig {
        IdlePoolConfig {
            name,
            min_idle,
            max_idle,
            max_total,
        }
    }

    pub fn spawn<P, F>(&self, producer: P, operation: F) -> IdlePool<P, F>
    where
        P: IdlePoolWorkProducer + Send + Sync + 'static,
        F: Fn(P::Out) -> Result<()> + Sync + Send + 'static,
    {
        let manager_config = self.clone();
        let state = IdlePoolState::new(producer, operation);
        let manager_state = state.clone();
        let thread = thread::spawn(move || manager_thread(manager_config, manager_state));
        let pool_config = self.clone();
        IdlePool::new(pool_config, state, thread)
    }
}

pub struct IdlePool<P, F>
where
    P: IdlePoolWorkProducer + Send + Sync + 'static,
    F: Fn(P::Out) -> Result<()> + Sync + Send + 'static,
{
    config: IdlePoolConfig,
    state: Arc<IdlePoolState<P, F>>,
    thread: JoinHandle<()>,
}

impl<P, F> IdlePool<P, F>
where
    P: IdlePoolWorkProducer + Send + Sync + 'static,
    F: Fn(P::Out) -> Result<()> + Sync + Send + 'static,
{
    fn new(
        config: IdlePoolConfig,
        state: Arc<IdlePoolState<P, F>>,
        thread: JoinHandle<()>,
    ) -> IdlePool<P, F> {
        IdlePool {
            config,
            state,
            thread,
        }
    }

    pub fn shutdown(&self) {
        trace!(target: "idle_queue_events", "pool={} shutting down", self.config.name);
        self.state.shutdown();
    }

    pub fn join(&self) {
        let id = self.thread.as_pthread_t();
        unsafe {
            libc::pthread_join(id, ptr::null_mut());
        }
    }
}
