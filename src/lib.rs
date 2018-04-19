extern crate futures;

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

trait HasMillis {
    fn as_millis(&self) -> u64;
}

impl HasMillis for Duration {
    fn as_millis(&self) -> u64 {
        return self.as_secs() * MILLIS_PER_SEC + ((self.subsec_nanos() / NANOS_PER_MILLI) as u64);
    }
}
struct TokenBucket {
    available_tokens: u32,
    waiting_wakers: Vec<Waker>
}

pub struct RateLimiter {
    token_bucket: Arc<Mutex<TokenBucket>>,
    is_still_adding_tokens: Arc<Mutex<bool>>
}

pub struct TokenFuture {
    token_bucket: Arc<Mutex<TokenBucket>>,
    token_acquired: bool
}

impl Drop for RateLimiter {
    fn drop(&mut self) {
        *self.is_still_adding_tokens.lock().unwrap() = false;
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
        let rate_limiter = RateLimiter {
            token_bucket: shared_token_bucket.clone(),
            is_still_adding_tokens: is_still_adding_tokens.clone()
        };
        let mut next_time_to_add_tokens = Instant::now() + time_between_adding_tokens;
        std::thread::spawn(move || {
            while *is_still_adding_tokens.lock().unwrap() {
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
            }
        });
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
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}
