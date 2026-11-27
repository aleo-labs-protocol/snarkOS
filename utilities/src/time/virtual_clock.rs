// Copyright (c) 2019-2026 Provable Inc.
// This file is part of the snarkOS library.

// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at:

// http://www.apache.org/licenses/LICENSE-2.0

// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! A clock for shuttle tests, which shuttle's `tokio` does not have.
//!
//! Each shuttle execution has its own clock, which starts at zero. The first timer of an
//! execution starts the clock task. Each time the clock task runs, it moves the clock to the
//! earliest deadline and wakes the timers that are due. Timers thus end in the order of their
//! deadlines, and no real time passes.
//!
//! When the clock task runs depends on the scheduler:
//! - Under [`IdleClock`], it runs only when no other task can run. A test then sees what it sees
//!   with a paused `tokio` clock: a task does all it can do at one instant before time moves.
//! - Under any other scheduler, it runs at any time. A timer can then end while another task
//!   is in the middle of its work.

use ::shuttle::{
    current,
    future,
    scheduler::{Schedule, Scheduler, Task, TaskId},
};
use std::{
    borrow::Cow,
    collections::BTreeMap,
    fmt,
    future::Future,
    ops::{Add, Sub},
    pin::Pin,
    sync::{
        Arc,
        Mutex,
        atomic::{AtomicBool, Ordering},
    },
    task::{Context, Poll, Waker},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

::shuttle::lazy_static! {
    /// The clock of the current execution. Shuttle makes a new one for each execution.
    ///
    /// The lock is never held across an `await`. Hazard: a shuttle lock here adds a point to
    /// switch tasks at each use of a timer.
    static ref CLOCK: Mutex<Clock> = Mutex::new(Clock::new());
}

/// The time of the clock at which it fails the execution. Hazard: with no limit, an execution in
/// which one task waits for an event that never comes, and another task keeps a timer, runs
/// until shuttle's limit of steps. That failure does not name a blocked task.
const MAX_TIME: Duration = Duration::from_secs(60 * 60);

/// The label of the clock task of an execution, which [`IdleClock`] holds back.
#[derive(Clone, Debug)]
struct ClockTask;

struct Clock {
    /// The system time at the start of the execution, in whole seconds.
    start: SystemTime,
    /// The time since the start of the execution.
    now: Duration,
    /// The number of timers made so far, which makes the keys of `timers` unique.
    num_timers: u64,
    /// The timers that are not due, by deadline.
    timers: BTreeMap<(Duration, u64), Timer>,
    /// Whether the clock task is spawned.
    has_task: bool,
    /// The waker of the clock task, while it waits for a timer.
    task_waker: Option<Waker>,
}

struct Timer {
    /// The waker of the task that waits for the timer.
    waker: Waker,
    /// Whether the [`Sleep`] of the timer is dropped.
    is_dropped: Arc<AtomicBool>,
}

impl Clock {
    fn new() -> Self {
        let start = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_secs();
        Self {
            start: UNIX_EPOCH + Duration::from_secs(start),
            now: Duration::ZERO,
            num_timers: 0,
            timers: BTreeMap::new(),
            has_task: false,
            task_waker: None,
        }
    }
}

/// Returns the system time. In an execution, it is the time at which the execution started, in
/// whole seconds, plus the time of its clock. A timestamp in seconds thus changes only when the
/// clock moves, and not with the real time that an execution takes.
///
/// Hazard: on a thread that shuttle does not run, such as a `rayon` thread, it is the real system
/// time. The timers panic there.
pub fn system_now() -> SystemTime {
    match current::try_get_current_task() {
        Some(_) => {
            let clock = CLOCK.lock().unwrap();
            clock.start + clock.now
        }
        None => SystemTime::now(),
    }
}

/// A point in time of the clock; see `tokio::time::Instant`.
#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct Instant(Duration);

impl Instant {
    pub fn now() -> Self {
        Self(CLOCK.lock().unwrap().now)
    }

    pub fn elapsed(&self) -> Duration {
        Self::now() - *self
    }

    pub fn duration_since(&self, earlier: Self) -> Duration {
        self.0.saturating_sub(earlier.0)
    }
}

impl Add<Duration> for Instant {
    type Output = Self;

    fn add(self, duration: Duration) -> Self {
        Self(self.0.saturating_add(duration))
    }
}

impl Sub for Instant {
    type Output = Duration;

    fn sub(self, earlier: Self) -> Duration {
        self.duration_since(earlier)
    }
}

/// A future that ends at its deadline; see `tokio::time::Sleep`.
#[derive(Debug)]
pub struct Sleep {
    deadline: Instant,
    /// The number of the timer in the clock and its dropped flag, once the first poll made it.
    timer: Option<(u64, Arc<AtomicBool>)>,
}

/// Waits until `duration` has elapsed; see `tokio::time::sleep`.
pub fn sleep(duration: Duration) -> Sleep {
    sleep_until(Instant::now() + duration)
}

/// Waits until `deadline`; see `tokio::time::sleep_until`.
pub fn sleep_until(deadline: Instant) -> Sleep {
    Sleep { deadline, timer: None }
}

impl Future for Sleep {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        let deadline = self.deadline.0;
        let (task_waker, spawn_task) = {
            let mut clock = CLOCK.lock().unwrap();
            let clock = &mut *clock;
            if clock.now >= deadline {
                return Poll::Ready(());
            }
            let (number, is_dropped) = self
                .timer
                .get_or_insert_with(|| {
                    clock.num_timers += 1;
                    (clock.num_timers, Arc::default())
                })
                .clone();
            clock.timers.insert((deadline, number), Timer { waker: cx.waker().clone(), is_dropped });
            (clock.task_waker.take(), !std::mem::replace(&mut clock.has_task, true))
        };
        if let Some(task_waker) = task_waker {
            task_waker.wake();
        }
        if spawn_task {
            // Shuttle ends an execution while this task still runs only if nothing can join it.
            drop(future::spawn(run_clock()));
        }
        Poll::Pending
    }
}

impl Drop for Sleep {
    /// Hazard: this must not use `CLOCK`. Shuttle drops the futures of an execution after the
    /// clock of that execution.
    fn drop(&mut self) {
        if let Some((_, is_dropped)) = &self.timer {
            is_dropped.store(true, Ordering::Relaxed);
        }
    }
}

/// Moves the clock from deadline to deadline, for as long as the execution runs.
async fn run_clock() {
    current::set_label_for_task(current::me(), ClockTask);
    loop {
        // Let the other tasks run. Under `IdleClock`, this task waits here for all of them.
        future::yield_now().await;
        NextDeadline.await;
    }
}

/// A future that moves the clock to the earliest deadline and wakes the timers that are due.
/// It waits for a timer if there is none.
struct NextDeadline;

impl Future for NextDeadline {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        let (now, due) = {
            let mut clock = CLOCK.lock().unwrap();
            let clock = &mut *clock;
            clock.timers.retain(|_, timer| !timer.is_dropped.load(Ordering::Relaxed));
            let Some((&(deadline, _), _)) = clock.timers.first_key_value() else {
                clock.task_waker = Some(cx.waker().clone());
                return Poll::Pending;
            };
            clock.now = clock.now.max(deadline);
            let not_due = clock.timers.split_off(&(clock.now, u64::MAX));
            (clock.now, std::mem::replace(&mut clock.timers, not_due))
        };
        assert!(now <= MAX_TIME, "the clock passed {MAX_TIME:?}: does a task wait for an event that never comes?");
        for timer in due.into_values() {
            timer.waker.wake();
        }
        Poll::Ready(())
    }
}

/// A future that ends with its inner future or at its deadline; see `tokio::time::Timeout`.
pub struct Timeout<F> {
    future: Pin<Box<F>>,
    sleep: Sleep,
}

/// Requires `future` to end before `duration` has elapsed; see `tokio::time::timeout`.
pub fn timeout<F: Future>(duration: Duration, future: F) -> Timeout<F> {
    timeout_at(Instant::now() + duration, future)
}

/// Requires `future` to end before `deadline`; see `tokio::time::timeout_at`.
pub fn timeout_at<F: Future>(deadline: Instant, future: F) -> Timeout<F> {
    Timeout { future: Box::pin(future), sleep: sleep_until(deadline) }
}

impl<F: Future> Future for Timeout<F> {
    type Output = Result<F::Output, error::Elapsed>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if let Poll::Ready(output) = self.future.as_mut().poll(cx) {
            return Poll::Ready(Ok(output));
        }
        Pin::new(&mut self.sleep).poll(cx).map(|()| Err(error::Elapsed(())))
    }
}

pub mod error {
    use super::fmt;

    /// The error of a [`Timeout`](super::Timeout) that ended at its deadline.
    #[derive(Debug, PartialEq, Eq)]
    pub struct Elapsed(pub(super) ());

    impl fmt::Display for Elapsed {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            "deadline has elapsed".fmt(f)
        }
    }

    impl std::error::Error for Elapsed {}
}

/// A scheduler that runs the clock task only when no other task can run, and that decides like
/// the scheduler it wraps otherwise.
pub struct IdleClock<S> {
    scheduler: S,
    /// Whether the wrapped scheduler ever had more than one task to choose from.
    had_choice: Arc<AtomicBool>,
    /// Whether a test ever took random data from the wrapped scheduler.
    took_random_data: Arc<AtomicBool>,
}

impl<S> IdleClock<S> {
    pub fn new(scheduler: S) -> Self {
        Self { scheduler, had_choice: Default::default(), took_random_data: Default::default() }
    }

    /// Returns a flag that is set once the wrapped scheduler has more than one task to choose
    /// from. If it is not set after an execution, the test has that one order only.
    pub fn had_choice(&self) -> Arc<AtomicBool> {
        self.had_choice.clone()
    }

    /// Returns a flag that is set once a test takes random data from the wrapped scheduler, as
    /// `shuttle::rand` does. A test that sets neither flag in an execution has that one execution
    /// only.
    pub fn took_random_data(&self) -> Arc<AtomicBool> {
        self.took_random_data.clone()
    }
}

impl<S: Scheduler> Scheduler for IdleClock<S> {
    fn new_execution(&mut self) -> Option<Schedule> {
        self.scheduler.new_execution()
    }

    fn next_task(
        &mut self,
        runnable_tasks: &[&Task],
        current_task: Option<TaskId>,
        is_yielding: bool,
    ) -> Option<TaskId> {
        let clock_task =
            runnable_tasks.iter().position(|task| current::get_label_for_task::<ClockTask>(task.id()).is_some());
        // The list also holds blocked tasks that can wake with no cause. They do not count: with
        // only those and the clock task in the list, the clock task must be able to run.
        let other_tasks = match clock_task {
            Some(clock_task)
                if runnable_tasks.iter().enumerate().any(|(i, task)| i != clock_task && task.runnable()) =>
            {
                let mut other_tasks = runnable_tasks.to_vec();
                other_tasks.remove(clock_task);
                Cow::Owned(other_tasks)
            }
            _ => Cow::Borrowed(runnable_tasks),
        };
        if other_tasks.len() > 1 {
            self.had_choice.store(true, Ordering::Relaxed);
        }
        self.scheduler.next_task(&other_tasks, current_task, is_yielding)
    }

    fn next_u64(&mut self) -> u64 {
        self.took_random_data.store(true, Ordering::Relaxed);
        self.scheduler.next_u64()
    }
}
