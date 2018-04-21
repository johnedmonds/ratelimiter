//! Rate limiter for rust. Acquire tokens as futures which are resolved when the token is ready.
//!
//!  # Example Acquiring a Token
//!
//! ```
//! extern crate ratelimiter;
//! extern crate futures;
//! use ratelimiter::{RateLimiter, TokenFuture};
//! use futures::executor::ThreadPool;
//! use std::time::Duration;
//!
//! use futures::FutureExt;
//! let mut rate_limiter = RateLimiter::new(1, 1, Duration::from_secs(1));
//! let token: TokenFuture = rate_limiter.acquire_token();
//! let and_then = token.and_then(|_| {
//!     println!("Token acquired!");
//!     return Ok(());
//! });
//! ThreadPool::new().unwrap().run(and_then);
//! ```

extern crate futures;

use std::thread::Thread;
use std::cmp::min;
use std::time::Duration;
use std::time::Instant;
use futures::Async::Pending;
use futures::Async::Ready;
use futures::task::Waker;
use futures::Async;
use futures::task::Context;
use std::sync::Arc;
use std::sync::Mutex;
use futures::prelude::Future;

const MILLIS_PER_SEC: u64 = 1_000;
const NANOS_PER_MILLI: u32 = 1_000_000;

/// Trait that exists because there's no way to get the number of milliseconds from a Duration.
trait HasMillis {
    fn as_millis(&self) -> u64;
}

impl HasMillis for Duration {
    fn as_millis(&self) -> u64 {
        return self.as_secs() * MILLIS_PER_SEC + ((self.subsec_nanos() / NANOS_PER_MILLI) as u64);
    }
}

/// Bucket of tokens and things waiting for those tokens.
struct TokenBucket {
    available_tokens: u32,
    waiting_wakers: Vec<Waker>
}

/// Limits the rate at which things can happen.
/// 
/// Tokens can be acquired as futures. A thread runs in the background adding tokens at a configurable rate. Once the RateLimiter is droped and there are no more futures to execute the token-adding thread terminates.
pub struct RateLimiter {
    token_bucket: Arc<Mutex<TokenBucket>>,
    /// True if the token-adding thread should keep adding tokens.
    is_still_adding_tokens: Arc<Mutex<bool>>,
    token_adding_thread: Thread
}

/// Future for a single token
/// 
/// Implementation note: You should not attempt to poll the future multiple times. Simply pass it to an executor to take care of it for you (this is because we record the waker used to immediately notify the futures that tokens are ready and polling multiple times registers multiple "wakers"). Ideally this should be fixed in another version.
pub struct TokenFuture {
    token_bucket: Arc<Mutex<TokenBucket>>,
    token_acquired: bool
}

impl Drop for RateLimiter {
    fn drop(&mut self) {
        // Allow the thread to exit.
        *self.is_still_adding_tokens.lock().unwrap() = false;
        // Wake up the thread immediately. It's possible there are more futures to complete so the thread won't stop but just in case this will allow the thread to terminate early.
        self.token_adding_thread.unpark();
    }
}

impl Future for TokenFuture {
    type Item = ();
    type Error = ();

    fn poll(&mut self, context: &mut Context) -> Result<Async<Self::Item>, Self::Error> {
        if self.token_acquired {
            return Err(());
        }
        let token_bucket: &mut TokenBucket = &mut self.token_bucket.lock().unwrap();
        if token_bucket.available_tokens > 0 {
            self.token_acquired = true;
            return Ok(Ready(()));
        }
        token_bucket.waiting_wakers.push(context.waker().clone());
        return Ok(Pending);
    }
}

impl RateLimiter {
    pub fn new(initial_tokens: u32, tokens_to_add: u32, time_between_adding_tokens: Duration) -> RateLimiter {
        let shared_token_bucket = Arc::new(Mutex::new(TokenBucket {
            available_tokens: initial_tokens,
            waiting_wakers: Vec::new()
        }));
        let is_still_adding_tokens = Arc::new(Mutex::new(true));
        let rate_limiter_token_bucket = shared_token_bucket.clone();
        let rate_limiter_is_still_adding_tokens = is_still_adding_tokens.clone();
        let mut next_time_to_add_tokens = Instant::now() + time_between_adding_tokens;
        let token_adding_thread = std::thread::spawn(move || {
            while *is_still_adding_tokens.lock().unwrap() || shared_token_bucket.lock().unwrap().waiting_wakers.len() > 0 {
                let now = Instant::now();
                if next_time_to_add_tokens > now {
                    let cycles_passed = 1 + (next_time_to_add_tokens.duration_since(now).as_millis() / time_between_adding_tokens.as_millis());
                    let token_bucket: &mut TokenBucket = &mut *shared_token_bucket.lock().unwrap();
                    token_bucket.available_tokens = token_bucket.available_tokens + (cycles_passed as u32) * tokens_to_add;
                    let wakers_to_wake = min(token_bucket.available_tokens as usize, token_bucket.waiting_wakers.len());
                    token_bucket.waiting_wakers.drain(0..wakers_to_wake).for_each(|waker: Waker| {
                        waker.wake();
                    });
                    next_time_to_add_tokens = next_time_to_add_tokens + time_between_adding_tokens * cycles_passed as u32;
                }
                std::thread::park_timeout(time_between_adding_tokens);
            }
        }).thread().clone();
        let rate_limiter = RateLimiter {
            token_bucket: rate_limiter_token_bucket,
            is_still_adding_tokens: rate_limiter_is_still_adding_tokens,
            token_adding_thread: token_adding_thread
        };
        return rate_limiter;
    }

    pub fn acquire_token(&mut self) -> TokenFuture {
        return TokenFuture {
            token_bucket: self.token_bucket.clone(),
            token_acquired: false
        };
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;
    use std::sync::Mutex;
    use futures::Async::Pending;
    use futures::Async::Ready;
    use futures::task::Context;
    use futures::task::LocalMap;
    use futures::executor::{LocalPool, ThreadPool};
    use futures::task::Wake;

    struct RecordingWake {
        woken: Mutex<bool>
    }

    impl Wake for RecordingWake {
        fn wake(arc_self: &Arc<Self>) {
            *arc_self.woken.lock().unwrap() = true;
        }
    }

    #[test]
    fn acquire_token() {
        let mut local_map = LocalMap::new();
        let local_pool = LocalPool::new();
        let mut executor = local_pool.executor();
        let recording_wake = RecordingWake {
            woken: Mutex::new(false)
        };
        let waker = Waker::from(Arc::new(recording_wake));
        let mut context = Context::new(&mut local_map, &waker, &mut executor);
        let mut rate_limiter = RateLimiter::new(1, 1, Duration::from_secs(1));
        assert_eq!(Ok(Ready(())), rate_limiter.acquire_token().poll(&mut context));
    }

    #[test]
    fn wait_for_token() {
        let mut local_map = LocalMap::new();
        let local_pool = LocalPool::new();
        let mut executor = local_pool.executor();
        let recording_wake = Arc::new(RecordingWake {
            woken: Mutex::new(false)
        });
        let waker = Waker::from(recording_wake.clone());
        let mut context = Context::new(&mut local_map, &waker, &mut executor);
        let sleep_duration = Duration::from_millis(100);
        let mut rate_limiter = RateLimiter::new(0, 1, sleep_duration);
        let mut token = rate_limiter.acquire_token();
        assert_eq!(Ok(Pending), token.poll(&mut context));
        assert_eq!(false, *recording_wake.woken.lock().unwrap());
        std::thread::sleep(sleep_duration * 2);
        assert_eq!(Ok(Ready(())), token.poll(&mut context));
        assert_eq!(true, *recording_wake.woken.lock().unwrap());
    }

    #[test]
    fn executor_test() {
        let mut pool = ThreadPool::new().unwrap();
        let mut rate_limiter = RateLimiter::new(0, 1, Duration::from_millis(100));
        assert_eq!(Ok(()), pool.run(rate_limiter.acquire_token()));
    }
}
