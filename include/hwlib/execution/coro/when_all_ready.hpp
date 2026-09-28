#pragma once

#include <atomic>
#include <coroutine>
#include <cstddef>
#include <hwlib/execution/coro/awaitable.hpp>
#include <hwlib/execution/coro/detail/result_store.hpp>
#include <tuple>
#include <type_traits>
#include <utility>

namespace hwlib::execution
{

template<typename T>
class WhenAllTask;

namespace detail
{

/// Counts the tasks still running, plus the awaiter itself: whoever brings it to
/// zero resumes the awaiter. It starts at the number of tasks; the awaiter's own
/// decrement is the last one when the tasks finished before it suspended.
class WhenAllCounter
{
public:
    explicit WhenAllCounter(std::size_t count) noexcept
        : m_count{count}
    {}

    /// Awaited once already: the tasks have run, and a second `co_await` of the
    /// same awaitable only hands their results back.
    [[nodiscard]] bool IsReady() const noexcept
    {
        return static_cast<bool>(m_continuation);
    }

    /// Whether to suspend: false when every task already finished.
    [[nodiscard]] bool TryAwait(std::coroutine_handle<> awaiter) noexcept
    {
        m_continuation = awaiter;
        return m_count.fetch_sub(1U, std::memory_order_acq_rel) > 0U;
    }

    [[nodiscard]] std::coroutine_handle<> NotifyCompleted() noexcept
    {
        if (m_count.fetch_sub(1U, std::memory_order_acq_rel) == 0U)
        {
            return m_continuation;
        }
        return std::noop_coroutine();
    }

private:
    std::atomic_size_t m_count;
    std::coroutine_handle<> m_continuation;
};

template<typename T>
struct WhenAllPromise : ResultStore<T>
{
    using Handle = std::coroutine_handle<WhenAllPromise>;

    WhenAllCounter* counter{nullptr};

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

            [[nodiscard]] std::coroutine_handle<> await_suspend(Handle finished) const noexcept
            {
                return finished.promise().counter->NotifyCompleted();
            }

            void await_resume() const noexcept {}
        };

        return Notifier{};
    }

    void unhandled_exception() noexcept
    {
        this->SetException();
    }

    template<typename V>
    [[nodiscard]] auto yield_value(V&& value)
    {
        this->SetValue(std::forward<V>(value));
        return final_suspend();
    }

    void Start(WhenAllCounter& taskCounter)
    {
        counter = &taskCounter;
        Handle::from_promise(*this).resume();
    }
};

template<>
struct WhenAllPromise<void> : ResultStore<void>
{
    using Handle = std::coroutine_handle<WhenAllPromise>;

    WhenAllCounter* counter{nullptr};

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

            [[nodiscard]] std::coroutine_handle<> await_suspend(Handle finished) const noexcept
            {
                return finished.promise().counter->NotifyCompleted();
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

    void Start(WhenAllCounter& taskCounter)
    {
        counter = &taskCounter;
        Handle::from_promise(*this).resume();
    }
};

template<typename Tasks>
class WhenAllReadyAwaitable;

} // namespace detail

/// One of the results WhenAllReady() hands back: its value, or the exception that
/// escaped it, read with Result().
template<typename T>
class WhenAllTask
{
public:
    using promise_type = detail::WhenAllPromise<T>;
    using Handle       = std::coroutine_handle<promise_type>;

    // NOLINTNEXTLINE(google-explicit-constructor): the coroutine's return object
    WhenAllTask(Handle handle) noexcept
        : m_handle{handle}
    {}

    WhenAllTask(const WhenAllTask&)            = delete;
    WhenAllTask& operator=(const WhenAllTask&) = delete;

    WhenAllTask(WhenAllTask&& other) noexcept
        : m_handle{std::exchange(other.m_handle, nullptr)}
    {}

    /// a139 overwrote the handle without destroying the frame it held.
    WhenAllTask& operator=(WhenAllTask&& other) noexcept
    {
        if (this != &other)
        {
            Destroy();
            m_handle = std::exchange(other.m_handle, nullptr);
        }
        return *this;
    }

    ~WhenAllTask()
    {
        Destroy();
    }

    [[nodiscard]] decltype(auto) Result() &
    {
        return m_handle.promise().Get();
    }

    [[nodiscard]] decltype(auto) Result() &&
    {
        return std::move(m_handle.promise()).Get();
    }

private:
    template<typename Tasks>
    friend class detail::WhenAllReadyAwaitable;

    void Start(detail::WhenAllCounter& counter)
    {
        m_handle.promise().Start(counter);
    }

    void Destroy() noexcept
    {
        if (m_handle)
        {
            m_handle.destroy();
        }
    }

    Handle m_handle;
};

namespace detail
{

template<>
class WhenAllReadyAwaitable<std::tuple<>>
{
public:
    constexpr WhenAllReadyAwaitable() noexcept = default;

    [[nodiscard]] constexpr bool await_ready() const noexcept
    {
        return true;
    }

    constexpr void await_suspend(std::coroutine_handle<>) const noexcept {}

    [[nodiscard]] constexpr std::tuple<> await_resume() const noexcept
    {
        return {};
    }
};

template<typename... Tasks>
class WhenAllReadyAwaitable<std::tuple<Tasks...>>
{
public:
    explicit WhenAllReadyAwaitable(Tasks&&... tasks) noexcept
        : m_counter{sizeof...(Tasks)}
        , m_tasks{std::move(tasks)...}
    {}

    WhenAllReadyAwaitable(const WhenAllReadyAwaitable&)            = delete;
    WhenAllReadyAwaitable& operator=(const WhenAllReadyAwaitable&) = delete;

    WhenAllReadyAwaitable(WhenAllReadyAwaitable&& other) noexcept
        : m_counter{sizeof...(Tasks)}
        , m_tasks{std::move(other.m_tasks)}
    {}

    WhenAllReadyAwaitable& operator=(WhenAllReadyAwaitable&&) = delete;
    ~WhenAllReadyAwaitable()                                  = default;

    [[nodiscard]] auto operator co_await() & noexcept
    {
        struct Awaiter
        {
            WhenAllReadyAwaitable& awaitable;

            [[nodiscard]] bool await_ready() const noexcept
            {
                return awaitable.m_counter.IsReady();
            }

            [[nodiscard]] bool await_suspend(std::coroutine_handle<> awaiter) noexcept
            {
                return awaitable.StartAll(awaiter);
            }

            [[nodiscard]] std::tuple<Tasks...>& await_resume() noexcept
            {
                return awaitable.m_tasks;
            }
        };

        return Awaiter{*this};
    }

    [[nodiscard]] auto operator co_await() && noexcept
    {
        struct Awaiter
        {
            WhenAllReadyAwaitable& awaitable;

            [[nodiscard]] bool await_ready() const noexcept
            {
                return awaitable.m_counter.IsReady();
            }

            [[nodiscard]] bool await_suspend(std::coroutine_handle<> awaiter) noexcept
            {
                return awaitable.StartAll(awaiter);
            }

            [[nodiscard]] std::tuple<Tasks...>&& await_resume() noexcept
            {
                return std::move(awaitable.m_tasks);
            }
        };

        return Awaiter{*this};
    }

private:
    [[nodiscard]] bool StartAll(std::coroutine_handle<> awaiter) noexcept
    {
        std::apply([this](auto&... tasks) { (tasks.Start(m_counter), ...); }, m_tasks);
        return m_counter.TryAwait(awaiter);
    }

    WhenAllCounter m_counter;
    std::tuple<Tasks...> m_tasks;
};

template<typename A, typename Result = typename AwaitableTraits<A&&>::AwaitResult>
WhenAllTask<StoredResult<Result>> MakeWhenAllTask(A awaitable)
{
    if constexpr (std::is_void_v<Result>)
    {
        co_await std::move(awaitable);
    }
    else
    {
        co_yield co_await std::move(awaitable);
    }
}

} // namespace detail

/// Awaits every awaitable, started one after another in the calling thread and
/// running concurrently from their first suspension, and resumes when the last
/// finishes. Evaluates to a tuple of WhenAllTask; nothing escapes it — each
/// task's Result() gives its value or rethrows its exception.
template<Awaitable... As>
[[nodiscard]] auto WhenAllReady(As&&... awaitables)
{
    using Tasks = std::tuple<
        WhenAllTask<detail::StoredResult<typename AwaitableTraits<std::remove_reference_t<As>>::AwaitResult>>...>;
    return detail::WhenAllReadyAwaitable<Tasks>{detail::MakeWhenAllTask(std::forward<As>(awaitables))...};
}

} // namespace hwlib::execution
