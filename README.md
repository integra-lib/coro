# coro

C++20 coroutines for firmware: a lazy `Task`, `SyncWait` to run one from ordinary
code, `WhenAllReady`, fire-and-forget `AsyncScope`, `AsyncGenerator`, and
`IoService` — a scheduler with timers, run by one thread. Plus two bounded queues,
`SpscQueue` and `MpmcQueue`.

Part of hwlib. Header-only, no platform code: the clock and the semaphore the
scheduler sleeps on are template parameters. Works with and without exceptions.

## Use it

```cmake
add_subdirectory(external/hwlib/coro)
target_link_libraries(app PRIVATE Hwlib::coro)
```

```cpp
#include <hwlib/execution/coro.hpp>

using namespace hwlib::execution;
using namespace std::chrono_literals;

// The scheduler: any semaphore with release() / try_acquire_until(), any clock.
IoService<std::binary_semaphore> g_io;   // std::chrono::steady_clock by default

Task<int> ReadSensor()
{
    co_await g_io.ScheduleAfter(100ms);   // resumes on g_io's thread, 100 ms later
    co_return 42;
}

Task<> Blink()
{
    for (;;)
    {
        Toggle();
        co_await g_io.ScheduleAfter(500ms);
    }
}

int main()
{
    // Everything the service thread touches is declared before it, so it is still
    // alive when the thread stops: jthread's destructor asks for the stop and joins.
    auto blink = ScheduleOn(g_io, Blink());
    blink.Resume();                                 // queued on g_io; runs on its thread
    BinaryEvent done;                               // Set() / Wait()

    std::jthread ioThread{[](std::stop_token stop) {
        // Run() looks at the stop request once a round; Wake() starts one now.
        const std::stop_callback wakeOnStop{stop, [] { g_io.Wake(); }};
        g_io.Run([&stop] { return stop.stop_requested(); }, 1s);
    }};

    const int value = SyncWait(ReadSensor(), done); // blocks this thread until it is done
}
```

| | Does |
|---|---|
| `Task<T>` | lazy: runs when awaited or `Resume()`d; `Result()`, `IsReady()`, `WhenReady()` |
| `SyncWait(awaitable, event)` | runs it from ordinary code, blocking on `event` (`Set()`/`Wait()`) |
| `WhenAllReady(a, b, …)` | awaits all; a tuple of `WhenAllTask` whose `Result()` gives each value or rethrows |
| `AsyncScope` | `Spawn()` starts work and forgets it, `co_await Join()` waits for all of it; `Spawn()` after `Join()`, or destroying the scope with work running, ends the program |
| `ScheduleOn(scheduler, awaitable)` | a task that first moves onto the scheduler |
| `AsyncGenerator<T>` | `for (auto it = co_await g.Begin(); it != g.End();) { …; co_await ++it; }` |
| `IoService<Semaphore, Clock>` | `Schedule()`, `ScheduleAt()`, `ScheduleAfter()`; `Run(stop, maxSleep)`, `Wake()` |
| `SpscQueue<T, N>`, `MpmcQueue<T, N>` | bounded, N a power of two; `TryPush(T&&)`, `TryPop()`; SPSC also blocking `Push`/`Pop` |
| `ThisCoroutine()`, `Awaitable`, `AwaitableTraits` | the handle of the calling coroutine; the concepts |

### IoService

One thread runs it — `Run()`, or `ProcessTimers()` and `ProcessOne()` in a loop of
its own. Any thread may schedule onto it, lock-free. Nothing is allocated: each
scheduled coroutine is a node in an intrusive list, inside its own frame, so there
is no capacity to overflow, and coroutines run in the order they were scheduled.
Timers run by deadline, and in scheduling order when tied.

`Run()` works in rounds: due timers, then what was ready when the round began,
then sleep. What is scheduled during a round waits for the next one, so a
coroutine that reschedules itself, or a thread flooding the service, cannot keep
`Run()` from its timers and its stop request.

The semaphore is released at most once between two acquisitions, so a binary one
is enough. On Zephyr, `k_sem` with `k_sem_give` and a `k_sem_take` until the
deadline fits; the clock can be `k_uptime_get()` behind a `std::chrono`-style
clock type.

### Exceptions

With exceptions enabled, an exception escaping a coroutine travels to whoever
awaits it — through `co_await`, `Result()` or `SyncWait()`. Built with
`-fno-exceptions`, nothing can escape, and a coroutine whose body would throw ends
the program; so does awaiting a moved-from `Task` (`BrokenPromise` otherwise).

## Examples

The examples below are compiled and run by `tests/readme_examples_test.cpp`, where
they carry test hooks — counters instead of LEDs, a bounded loop instead of an
endless one — around the same code. The last, the Zephyr adapters, is a sketch
not built on the host. `Io` is `IoService<std::binary_semaphore>`, and `done` a
`BinaryEvent`: a binary semaphore behind `Set()` and `Wait()`.

### Tasks: values and chaining

```cpp
Task<int> ReadRaw()
{
    co_return 1234;
}

Task<float> ReadVolts()
{
    const int raw = co_await ReadRaw();   // runs ReadRaw() to its end, here
    co_return static_cast<float>(raw) * 0.001F;
}

const float volts = SyncWait(ReadVolts(), done);   // 1.234
```

A `Task` does nothing until it is awaited or resumed, so creating one and dropping
it costs a frame and runs no code.

### A periodic job

```cpp
Task<> Blink(Io& io)
{
    for (;;)
    {
        Toggle(led);
        co_await io.ScheduleAfter(500ms);   // the thread is free meanwhile
    }
}

auto blink = ScheduleOn(io, Blink(io));   // keep it alive as long as it runs
blink.Resume();
```

### Waiting for several things at once

```cpp
Task<int> ReadTemperature(Io& io)
{
    co_await io.ScheduleAfter(10ms);   // conversion time
    co_return 21;
}

Task<int> ReadBoth(Io& io)
{
    // Both start now and wait in parallel: about 10 ms, not 20.
    auto [temperature, humidity] = co_await WhenAllReady(ReadTemperature(io), ReadHumidity(io));
    co_return temperature.Result() * 100 + humidity.Result();
}
```

Each element is a `WhenAllTask`: `Result()` gives its value, or rethrows what
escaped it, one by one.

### Fire and forget, then wait for all of it

```cpp
Task<> HandleAll(Io& io)
{
    AsyncScope scope;
    for (int request = 1; request <= 5; ++request)
    {
        scope.Spawn(Handle(io, request));   // started now, not awaited
    }
    co_await scope.Join();                  // before the scope goes away
}
```

Destroying the scope before `Join()` has returned, or spawning after it, ends the
program: the work would finish into a dead scope.

### A stream of values

```cpp
AsyncGenerator<int> Samples(Io& io, int count)
{
    for (int i = 0; i < count; ++i)
    {
        co_await io.ScheduleAfter(2ms);   // wait for the next sample
        co_yield i * 10;
    }
}

Task<int> Average(Io& io)
{
    int sum = 0;
    int count = 0;
    auto samples = Samples(io, 4);
    for (auto it = co_await samples.Begin(); it != samples.End();)
    {
        sum += *it;
        ++count;
        co_await ++it; // in the body: GCC 13 rejects it as the loop's increment in a template
    }
    co_return count == 0 ? 0 : sum / count;
}
```

### Moving between threads

Two services, as in a139: a fast thread for short work, a slow one for work that
blocks. `co_await` on another service's `Schedule()` continues there.

```cpp
Task<> SaveSettings(Io& fast, Io& slow, Settings settings)
{
    co_await fast.Schedule();
    Validate(settings);        // quick, on the fast thread
    co_await slow.Schedule();
    WriteToFlash(settings);    // blocks for milliseconds, on the slow thread
    co_await fast.Schedule();
    Notify();                  // back
}
```

### From an interrupt to a coroutine

```cpp
MpmcQueue<std::uint16_t, 16> g_adcSamples;
std::atomic<std::uint32_t> g_adcDropped{0};

void AdcInterrupt(std::uint16_t sample)
{
    // Never waits: fails, as if full, while the consumer holds the lock — so count
    // the loss rather than hide it.
    if (!g_adcSamples.TryPush(std::uint16_t{sample}))
    {
        ++g_adcDropped;
    }
}

Task<int> CollectSamples(Io& io, const std::atomic<bool>& adcStopped)
{
    int sum = 0;
    for (;;)
    {
        if (const auto sample = g_adcSamples.TryPop(); sample.has_value())
        {
            sum += *sample;
        }
        else if (adcStopped.load())
        {
            co_return sum;   // stopped and drained
        }
        else
        {
            co_await io.ScheduleAfter(1ms);   // nothing yet: let the thread do other work
        }
    }
}
```

### Errors

```cpp
Task<int> Tolerant()
{
    try
    {
        co_return co_await ReadSensor();
    }
    catch (const std::runtime_error&)
    {
        co_return -1;   // the exception arrived where the task was awaited
    }
}
```

Without exceptions, return the error as a value — `Task<std::optional<int>>` or an
expected-like type — since nothing can escape a coroutine.

### Zephyr adapters (sketch, not built on the host)

```cpp
/// k_uptime as a steady std::chrono clock.
struct UptimeClock
{
    using rep        = std::int64_t;
    using period     = std::milli;
    using duration   = std::chrono::duration<rep, period>;
    using time_point = std::chrono::time_point<UptimeClock>;
    static constexpr bool is_steady = true;

    static time_point now() noexcept { return time_point{duration{k_uptime_get()}}; }
};

/// k_sem as the service's wake semaphore; k_sem_give() is safe from an ISR.
class KernelSemaphore
{
public:
    explicit KernelSemaphore(std::ptrdiff_t initial) noexcept
    {
        k_sem_init(&m_sem, static_cast<unsigned>(initial), 1U);
    }
    void release() noexcept { k_sem_give(&m_sem); }
    bool try_acquire_until(UptimeClock::time_point until) noexcept
    {
        const auto left = until - UptimeClock::now();
        return k_sem_take(&m_sem, left.count() > 0 ? K_MSEC(left.count()) : K_NO_WAIT) == 0;
    }

private:
    k_sem m_sem;
};

/// SyncWait()'s event. In Zephyr's kernel/sem.c, k_sem_give() does not touch the
/// semaphore after making the waiter ready — its lock is file-wide — except for
/// the tracing hook when object tracing is on; then keep the event static.
class KernelEvent
{
public:
    KernelEvent() noexcept { k_sem_init(&m_sem, 0U, 1U); }
    void Set() noexcept { k_sem_give(&m_sem); }
    void Wait() noexcept { std::ignore = k_sem_take(&m_sem, K_FOREVER); }

private:
    k_sem m_sem;
};

IoService<KernelSemaphore, UptimeClock> g_io;

// In a thread of its own, with a stack sized for the coroutines it resumes:
void IoThread(void*, void*, void*)
{
    g_io.Run([] { return false; }, std::chrono::seconds{1});
}
```

## What to know on a microcontroller

* **Coroutine frames are allocated with `operator new`**, as the language defines
  it; the compiler may elide a frame that does not outlive its caller. Provide a
  heap, or keep the long-lived coroutines few and started once.
* **Lock-free atomics are required** — compare-and-swap in the scheduler, the
  counters of `WhenAllReady` and `AsyncScope`. Cortex-M3 and up have them; on a
  Cortex-M0 the build calls `__atomic_*` library functions, which need libatomic or
  an implementation that masks interrupts.
* `MpmcQueue` never waits: its `TryPush`/`TryPop` fail, as if full or empty, while
  another context holds its lock, which makes them safe from an interrupt.
* **GCC 13 rejects `co_await` in a `for` loop's increment inside a template**
  ("insufficient contextual information to determine type"); GCC 15 and clang
  accept it. Put `co_await ++it;` at the end of the loop body, as the examples do.
* **clang reuses `std::this_thread::get_id()` across a `co_await`** at `-O1` and
  above — it treats `pthread_self()` as constant — although the coroutine may have
  moved to another thread there. The tests caught it; read thread identity, and
  `thread_local` state, through a call the optimiser cannot see into.
* A capturing coroutine lambda must outlive its coroutine: its captures live in the
  closure, not in the frame. Keep the lambda in a variable, or pass by parameter.
* **`SyncWait()`'s event must outlive its `Set()`.** The waiter can wake and return
  while `Set()` is still inside the other thread's `release()` — a
  `std::binary_semaphore` notifies after it counts. An event on the caller's stack
  needs a `Set()` that is done with the object once the waiter can see it (a
  Zephyr `k_sem_give()` is), or must be kept alive until that thread is done.

## Coming from a139-bms48v-firmware

The library is a139's `lib/coro`, itself modelled on cppcoro. The names follow
hwlib; the language's own (`promise_type`, `await_ready`, …) are unchanged. Each
defect below was confirmed by building a139's headers and running the scenario.

* **`io_service` lost coroutines.** When its 32-slot work queue was full, it put
  the overflow back on its intrusive list by the tail instead of the head: of 100
  coroutines scheduled at once, 33 ever ran. The scheduler no longer has a
  bounded queue at all (above), and so nothing to overflow.
* **`async_scope::join()` did not wait for a single remaining work.** The count
  started at 0 where cppcoro's starts at 1, but `join()` kept cppcoro's `> 1`: with
  exactly one work running, the joiner went straight on, and the work's end resumed
  it a second time. The count starts at 1 again.
* **`task.hpp` broke the link of any program that included it twice**:
  `promise<void>::get_return_object()` was defined in the header without `inline`
  (`multiple definition of coro::detail::promise<void>::get_return_object()`).
* **Move assignment leaked a coroutine frame** in `when_all_task` and
  `async_generator`: the handle was overwritten without destroying the frame.
* **The queues never destroyed what was left in them** — no destructor.
* **`sync_wait()` on a task returning `T&` did not compile**; a reference result is
  now kept as a reference and returned as a copy.
* **The scheduler's clock was `high_resolution_clock`**, which libstdc++ defines as
  the system clock: setting the time moved every timer. The clock is a parameter,
  `steady_clock` by default, and one whose `is_steady` is false does not compile.
* **The semaphore could be released past its maximum**: `counting_semaphore<1024>`
  got a `release()` per scheduled coroutine, undefined beyond 1024 pending. Wakes
  are now coalesced into one.
* **C++23 and exceptions were required**: `std::unreachable`,
  `std::optional::transform`, `[this] noexcept` lambdas, `throw` in the task. The
  component is C++20 and builds with `-fno-exceptions -fno-rtti`.
* `try_push(T&)` moved from the caller's lvalue; `TryPush` takes an rvalue and
  leaves the value alone when it fails. `async_generator<T&&>` declared a
  `get_return_object()` it never defined; `AsyncGenerator<T&&>` now behaves as
  `AsyncGenerator<T>`. `scoped_lambda` / `on_scope_*` became an internal detail.
* Undefined behaviour a139 left to the caller now ends the program: `Resume()` on a
  finished task, reading a task's result before it finished, `spawn()` after
  `join()`, destroying a scope with work running; `Result()` on a moved-from task
  is a `BrokenPromise`.
* The scheduler's `thread_func(std::stop_token, …)` is `Run(predicate, maxSleep)`
  with `Wake()`, which needs neither `<stop_token>` nor threads in the library.

An external review found the round-less `Run()`, the unguarded scope, the unchecked
`Resume()` and `Result()`, and a hanging example in this README; all are fixed.

Not yet run on a device: built for Cortex-M4F with `-fno-exceptions -fno-rtti`, and
tested on the host under ASan, UBSan and TSan.

## Develop it

```bash
git submodule update --init          # ci-shared, needed by pre-commit
cmake -S . -B build && cmake --build build -j && ctest --test-dir build
```
