#pragma once

#include <coroutine>
#include <hwlib/execution/coro/awaitable.hpp>
#include <hwlib/execution/coro/detail/result_store.hpp>
#include <type_traits>
#include <utility>

namespace hwlib::execution
{

/// A one-shot event SyncWait() blocks on: Set() from the coroutine's thread,
/// Wait() on the caller's. A binary semaphore wrapped in two lines is enough.
template<typename E>
concept SyncEvent = requires(E& event) {
    event.Set();
    event.Wait();
};

namespace detail
{

template<typename T, typename Event>
class SyncWaitTask;

template<typename T, typename Event>
struct SyncWaitPromise : ResultStore<T>
{
    using Handle = std::coroutine_handle<SyncWaitPromise>;

    Event* event{nullptr};

    [[nodiscard]] Handle get_return_object() noexcept
    {
        return Handle::from_promise(*this);
    }

    [[nodiscard]] std::suspend_always initial_suspend() const noexcept
    {
        return {};
    }

    [[nodiscard]] auto final_suspend() const noexcept
    {
        struct Notifier
        {
            [[nodiscard]] bool await_ready() const noexcept
            {
                return false;
            }

            void await_suspend(Handle finished) const noexcept
            {
                // Suspended already ([expr.await]); the waiter destroys this frame
                // once Set() lets it go, so the event pointer is read first.
                Event* const event = finished.promise().event;
                event->Set();
            }

            void await_resume() const noexcept {}
        };

        return Notifier{};
    }

    void unhandled_exception() noexcept
    {
        this->SetException();
    }

    /// The awaited result arrives by co_yield, forwarded. A reference result is kept
    /// as a reference and copied out by SyncWait(): in a139 SyncWait() on a task
    /// returning `T&` did not compile.
    template<typename V>
    [[nodiscard]] auto yield_value(V&& value)
    {
        this->SetValue(std::forward<V>(value));
        return final_suspend();
    }

    // No return_void(): the body ends in `co_yield`, which suspends for good, and
    // flowing off the end is only undefined when it happens
    // ([stmt.return.coroutine]).
};

template<typename Event>
struct SyncWaitPromise<void, Event> : ResultStore<void>
{
    using Handle = std::coroutine_handle<SyncWaitPromise>;

    Event* event{nullptr};

    [[nodiscard]] Handle get_return_object() noexcept
    {
        return Handle::from_promise(*this);
    }

    [[nodiscard]] std::suspend_always initial_suspend() const noexcept
    {
        return {};
    }

    [[nodiscard]] auto final_suspend() const noexcept
    {
        struct Notifier
        {
            [[nodiscard]] bool await_ready() const noexcept
            {
                return false;
            }

            void await_suspend(Handle finished) const noexcept
            {
                // Suspended already ([expr.await]); the waiter destroys this frame
                // once Set() lets it go, so the event pointer is read first.
                Event* const event = finished.promise().event;
                event->Set();
            }

            void await_resume() const noexcept {}
        };

        return Notifier{};
    }

    void unhandled_exception() noexcept
    {
        this->SetException();
    }

    void return_void() const noexcept {}
};

template<typename T, typename Event>
class SyncWaitTask
{
public:
    using promise_type = SyncWaitPromise<T, Event>;
    using Handle       = std::coroutine_handle<promise_type>;

    // NOLINTNEXTLINE(google-explicit-constructor): the coroutine's return object
    SyncWaitTask(Handle handle) noexcept
        : m_handle{handle}
    {}

    SyncWaitTask(const SyncWaitTask&)            = delete;
    SyncWaitTask& operator=(const SyncWaitTask&) = delete;
    SyncWaitTask(SyncWaitTask&&)                 = delete;
    SyncWaitTask& operator=(SyncWaitTask&&)      = delete;

    ~SyncWaitTask()
    {
        m_handle.destroy();
    }

    void Start(Event& event)
    {
        m_handle.promise().event = &event;
        m_handle.resume();
    }

    [[nodiscard]] decltype(auto) Result()
    {
        return std::move(m_handle.promise()).Get();
    }

private:
    Handle m_handle;
};

template<typename Event, typename A, typename Result = typename AwaitableTraits<A&&>::AwaitResult>
SyncWaitTask<StoredResult<Result>, Event> MakeSyncWaitTask(A&& awaitable)
{
    if constexpr (std::is_void_v<Result>)
    {
        co_await std::forward<A>(awaitable);
    }
    else
    {
        co_yield co_await std::forward<A>(awaitable);
    }
}

} // namespace detail

/// Runs an awaitable to completion from ordinary code and returns its result,
/// blocking on `event` while it is suspended. The value is returned by value.
template<Awaitable A, SyncEvent Event>
decltype(auto) SyncWait(A&& awaitable, Event& event)
{
    using Result = typename AwaitableTraits<A&&>::AwaitResult;
    auto task    = detail::MakeSyncWaitTask<Event>(std::forward<A>(awaitable));
    task.Start(event);
    event.Wait();
    if constexpr (std::is_void_v<Result>)
    {
        task.Result();
    }
    else
    {
        return static_cast<std::remove_cvref_t<Result>>(task.Result());
    }
}

} // namespace hwlib::execution
