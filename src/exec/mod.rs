//! Implementation of the main benchmarking loop

use futures::channel::mpsc::{channel, Receiver, Sender};
use futures::{pin_mut, SinkExt, Stream, StreamExt};
use itertools::Itertools;
use pin_project::pin_project;
use status_line::StatusLine;
use std::f64::consts;
use std::future::ready;
use std::num::NonZeroUsize;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};
use tokio::pin;
use tokio::signal::ctrl_c;

use crate::error::{LatteError, Result};
use crate::{
    BenchmarkStats, BoundedCycleCounter, Interval, Progress, Recorder, Workload, WorkloadStats,
};
use chunks::ChunksExt;

mod chunks;
pub mod cycle;
pub mod progress;
pub mod workload;

/// Infinite iterator returning floats that form a sinusoidal wave
pub struct InfiniteSinusoidalIterator {
    rate: f64,
    amplitude: f64,
    step: f64,
    start: Instant,
}

impl InfiniteSinusoidalIterator {
    pub fn new(rate: f64, amplitude: f64, frequency: f64) -> InfiniteSinusoidalIterator {
        let step = consts::PI * 2.0 * frequency;
        InfiniteSinusoidalIterator {
            rate,
            amplitude,
            step,
            start: Instant::now(),
        }
    }
}

impl Iterator for InfiniteSinusoidalIterator {
    type Item = f64;

    fn next(&mut self) -> Option<f64> {
        if self.amplitude == 0.0 {
            return Some(self.rate);
        }
        let elapsed = self.start.elapsed().as_secs_f64();
        let adjusted_rate = self.rate + self.amplitude * (self.step * elapsed).sin();
        Some(adjusted_rate)
    }
}

/// Custom interval stream for sinusoidal ticking.
struct SinusoidalIntervalStream {
    tick_iterator: InfiniteSinusoidalIterator,
    next_expected_tick: Instant,
}

impl SinusoidalIntervalStream {
    fn new(rate: f64, amplitude: f64, frequency: f64) -> Self {
        let tick_iterator = InfiniteSinusoidalIterator::new(rate, amplitude, frequency);
        let now = Instant::now();
        let initial_duration = tokio::time::Duration::from_secs_f64(1.0 / rate);
        Self {
            tick_iterator,
            next_expected_tick: now + initial_duration,
        }
    }

    fn next_interval_duration(&mut self) -> tokio::time::Duration {
        let adjusted_rate = self.tick_iterator.next();
        let period_ns = (1_000_000_000.0 / adjusted_rate.unwrap_or(1.0)).max(1.0) as u64;
        tokio::time::Duration::from_nanos(period_ns)
    }
}

impl Stream for SinusoidalIntervalStream {
    // NOTE: pass through the 'scheduled time' for further
    //       coordinated omission fixed latency calculations.
    type Item = Instant;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let now = Instant::now();
        let current_expected_tick = self.next_expected_tick;
        if now >= self.next_expected_tick {
            let interval_duration = self.next_interval_duration();
            self.next_expected_tick += interval_duration;
            Poll::Ready(Some(current_expected_tick))
        } else {
            // NOTE: If we are behind, keep trying to emit ticks until we catch up
            let interval_duration = self.next_interval_duration();
            let next_tick = self.next_expected_tick + interval_duration;
            if next_tick <= now {
                self.next_expected_tick = next_tick;
                Poll::Ready(Some(current_expected_tick))
            } else {
                // NOTE: If we are ahead, sleep for the remaining duration
                let sleep_duration = self.next_expected_tick - now;
                let waker = cx.waker().clone();
                tokio::spawn(async move {
                    tokio::time::sleep(sleep_duration).await;
                    waker.wake();
                });
                Poll::Pending
            }
        }
    }
}

/// Returns a stream emitting sinusoidally changing number of `rate` events per second.
fn sinusoidal_interval_stream(
    rate: f64,
    amplitude: f64,
    frequency: f64,
) -> impl Stream<Item = Instant> {
    SinusoidalIntervalStream::new(rate, amplitude, frequency)
}

/// Runs a stream of workload cycles till completion in the context of the current task.
/// Periodically sends workload statistics to the `out` channel.
///
/// # Parameters
/// - stream: a stream of cycle numbers; None means the end of the stream
/// - workload: defines the function to call
/// - cycle_counter: shared cycle numbers provider
/// - concurrency: the maximum number of pending workload calls
/// - sampling: controls when to output workload statistics
/// - progress: progress bar notified about each successful cycle
/// - out: the channel to receive workload statistics
///
async fn run_stream(
    stream: impl Stream<Item = Instant> + std::marker::Unpin,
    workload: Workload,
    cycle_counter: BoundedCycleCounter,
    concurrency: NonZeroUsize,
    sampling: Interval,
    progress: Arc<StatusLine<Progress>>,
    mut out: Sender<Result<WorkloadStats>>,
) {
    let mut iter_counter = cycle_counter;
    let sample_size = sampling.count().unwrap_or(u64::MAX);
    let sample_duration = sampling.period().unwrap_or(tokio::time::Duration::MAX);

    let stats_stream = stream
        .map(|scheduled_time| iter_counter.next().map(|cycle| (cycle, scheduled_time)))
        .take_while(|opt| ready(opt.is_some()))
        .map(|opt| {
            let (cycle, scheduled_time) = opt.unwrap();
            tokio::task::unconstrained(workload.run(cycle, scheduled_time))
        })
        .buffer_unordered(concurrency.get())
        .inspect(|_| progress.tick())
        .take_until(ctrl_c())
        .terminate_after_error()
        .chunks_aggregated(sample_size, sample_duration, Vec::new, |errors, result| {
            if let Err(e) = result {
                errors.push(e)
            }
        })
        .map(|errors| (workload.take_stats(Instant::now()), errors));

    pin_mut!(stats_stream);

    workload.reset(Instant::now());
    while let Some((stats, errors)) = stats_stream.next().await {
        if out.send(Ok(stats)).await.is_err() {
            return;
        }
        for err in errors {
            if out.send(Err(err)).await.is_err() {
                return;
            }
        }
    }
}

/// Launches a new worker task that runs a series of invocations of the workload function.
///
/// The task will run as long as `deadline` produces new cycle numbers.
/// The task updates the `progress` bar after each successful cycle.
///
/// Returns a stream where workload statistics are published.
#[allow(clippy::too_many_arguments)]
fn spawn_stream(
    concurrency: NonZeroUsize,
    rate: Option<f64>,
    rate_sine_amplitude: Option<f64>, // Enables the sine wave if set
    rate_sine_frequency: f64,
    sampling: Interval,
    workload: Workload,
    iter_counter: BoundedCycleCounter,
    progress: Arc<StatusLine<Progress>>,
) -> Receiver<Result<WorkloadStats>> {
    let (tx, rx) = channel(1);

    tokio::spawn(async move {
        match rate {
            Some(rate) => {
                // NOTE: if 'rate_sine_amplitude' is empty or 0.0
                //       then it will behave like common uniform rate limiter.
                let stream = sinusoidal_interval_stream(
                    rate,
                    rate_sine_amplitude.unwrap_or(0.0) * rate, // transform to absolute value
                    rate_sine_frequency,
                );
                run_stream(
                    stream,
                    workload,
                    iter_counter,
                    concurrency,
                    sampling,
                    progress,
                    tx,
                )
                .await
            }
            None => {
                let stream = futures::stream::repeat_with(Instant::now);
                run_stream(
                    stream,
                    workload,
                    iter_counter,
                    concurrency,
                    sampling,
                    progress,
                    tx,
                )
                .await
            }
        }
    });
    rx
}

/// Receives one item from each of the streams.
/// Streams that are closed are ignored.
async fn receive_one_of_each<Instant, S>(streams: &mut [S]) -> Vec<Instant>
where
    S: Stream<Item = Instant> + Unpin,
{
    let mut items = Vec::with_capacity(streams.len());
    for s in streams {
        if let Some(item) = s.next().await {
            items.push(item);
        }
    }
    items
}

/// Controls the intensity of requests sent to the server
pub struct ExecutionOptions {
    /// How long to execute
    pub duration: Interval,
    /// Range of the cycle counter
    pub cycle_range: (i64, i64),
    /// Maximum rate of requests in requests per second, `None` means no limit
    pub rate: Option<f64>,
    /// Rate sine wave amplitude
    pub rate_sine_amplitude: Option<f64>,
    /// Rate sine wave period
    pub rate_sine_period: Duration,
    /// Number of parallel threads of execution
    pub threads: NonZeroUsize,
    /// Number of outstanding async requests per each thread
    pub concurrency: NonZeroUsize,
}

/// Executes the given function many times in parallel.
/// Draws a progress bar.
/// Returns the statistics such as throughput or duration histogram.
///
/// # Parameters
///   - `name`: text displayed next to the progress bar
///   - `count`: number of cycles
///   - `exec_options`: controls execution options such as parallelism level and rate
///   - `workload`: encapsulates a set of queries to execute
pub async fn par_execute(
    name: &str,
    exec_options: &ExecutionOptions,
    sampling: Interval,
    workload: Workload,
    show_progress: bool,
    keep_log: bool,
) -> Result<BenchmarkStats> {
    if exec_options.cycle_range.1 <= exec_options.cycle_range.0 {
        return Err(LatteError::Configuration(format!(
            "End cycle {} must not be lower than start cycle {}",
            exec_options.cycle_range.1, exec_options.cycle_range.0
        )));
    }

    let thread_count = exec_options.threads.get();
    let concurrency = exec_options.concurrency;
    let rate = exec_options.rate;
    let rate_sine_amplitude = exec_options.rate_sine_amplitude;
    let rate_sine_frequency = 1.0 / exec_options.rate_sine_period.as_secs_f64();
    let progress = match exec_options.duration {
        Interval::Count(count) => Progress::with_count(name.to_string(), count),
        Interval::Time(duration) => Progress::with_duration(name.to_string(), duration),
        Interval::Unbounded => unreachable!(),
    };
    let progress_opts = status_line::Options {
        initially_visible: show_progress,
        ..Default::default()
    };
    let progress = Arc::new(StatusLine::with_options(progress, progress_opts));
    let deadline = BoundedCycleCounter::new(exec_options.duration, exec_options.cycle_range);
    let mut streams = Vec::with_capacity(thread_count);
    let mut stats = Recorder::start(rate, concurrency, keep_log);

    for _ in 0..thread_count {
        let s = spawn_stream(
            concurrency,
            rate.map(|r| r / (thread_count as f64)),
            rate_sine_amplitude,
            rate_sine_frequency,
            sampling,
            workload.clone()?,
            deadline.share(),
            progress.clone(),
        );
        streams.push(s);
    }

    loop {
        let partial_stats = receive_one_of_each(&mut streams).await;
        let partial_stats: Vec<_> = partial_stats.into_iter().try_collect()?;
        if partial_stats.is_empty() {
            break Ok(stats.finish());
        }

        let aggregate = stats.record(&partial_stats);
        if sampling.is_bounded() {
            progress.set_visible(false);
            println!("{aggregate}");
            progress.set_visible(show_progress);
        }
    }
}

trait TerminateAfterErrorExt: Stream + Sized {
    /// Terminates the stream immediately after returning the first error.
    fn terminate_after_error(self) -> TerminateAfterError<Self>;
}

impl<S, Item, E> TerminateAfterErrorExt for S
where
    S: Stream<Item = std::result::Result<Item, E>>,
{
    fn terminate_after_error(self) -> TerminateAfterError<Self> {
        TerminateAfterError {
            stream: self,
            error: false,
        }
    }
}

#[pin_project]
struct TerminateAfterError<S: Stream> {
    #[pin]
    stream: S,
    error: bool,
}

impl<S, Item, E> Stream for TerminateAfterError<S>
where
    S: Stream<Item = std::result::Result<Item, E>>,
{
    type Item = S::Item;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.error {
            return Poll::Ready(None);
        }
        let this = self.project();
        match this.stream.poll_next(cx) {
            Poll::Ready(Some(Err(e))) => {
                *this.error = true;
                Poll::Ready(Some(Err(e)))
            }
            other => other,
        }
    }
}

#[cfg(test)]
mod test {
    use crate::exec::TerminateAfterErrorExt;
    use futures::stream;
    use futures::StreamExt;

    #[tokio::test]
    async fn test_terminate() {
        let s = stream::iter(vec![Ok(1), Ok(2), Err(3), Ok(4), Err(5)]).terminate_after_error();
        assert_eq!(s.collect::<Vec<_>>().await, vec![Ok(1), Ok(2), Err(3)])
    }
}
