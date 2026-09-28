#pragma once

#include <atomic>
#include <coroutine>
#include <cstddef>
#include <exception>
#include <hwlib/execution/coro/awaitable.hpp>
#include <hwlib/execution/coro/detail/result_store.hpp>
#include <hwlib/execution/coro/task.hpp>
#include <type_traits>
#include <utility>

namespace hwlib::execution
{

/// Fire-and-forget work that can still be waited for: Spawn() starts an awaitable
/// right away and forgets it, Join() resumes once everything spawned has finished.
///
/// The count starts at one — Join()'s own share — so the last of the spawned
/// works and Join() cannot both think they are last. a139's started at zero and
/// kept cppcoro's `> 1` in Join(): with exactly one work running, Join() did not
/// wait, and the work's end resumed the joiner a second time.
///
/// Join() after the last Spawn(); a second Join() returns at once. Ending the
/// program instead of undefined behaviour: Spawn() after Join(), destroying the
/// scope with work still running, and something escaping a spawned work — there
/// is no one to hand it to.
class AsyncScope
{
public:
    AsyncScope()                             = default;
    AsyncScope(const AsyncScope&)            = delete;
    AsyncScope& operator=(const AsyncScope&) = delete;
    AsyncScope(AsyncScope&&)                 = delete;
    AsyncScope& operator=(AsyncScope&&)      = delete;

    /// Work still running would finish into a dead scope.
    ~AsyncScope()
    {
        if (m_count.load(std::memory_order_acquire) > 1U)
        {
            std::terminate();
        }
    }

    template<Awaitable A>
    void Spawn(A&& work)
    {
        [](AsyncScope* scope, std::decay_t<A> awaitable) -> DetachedTask {
            scope->OnWorkStarted();
            const detail::ScopeExit finished{[scope]() noexcept { scope->OnWorkFinished(); }};
            co_await std::move(awaitable);
        }(this, std::forward<A>(work));
    }

    [[nodiscard]] auto Join() noexcept
    {
        struct Awaiter
        {
            AsyncScope& scope;

            [[nodiscard]] bool await_ready() const noexcept
            {
                return scope.m_count.load(std::memory_order_acquire) == 0U;
            }

            [[nodiscard]] bool await_suspend(std::coroutine_handle<> joiner) noexcept
            {
                // The joiner counts as suspended before this runs ([expr.await]):
                // the last work may resume it — on its own thread — as soon as the
                // count drops, so only the result of fetch_sub is used after it.
                AsyncScope& owner    = scope;
                owner.m_continuation = joiner;
                return owner.m_count.fetch_sub(1U, std::memory_order_acq_rel) > 1U;
            }

            void await_resume() const noexcept {}
        };

        return Awaiter{*this};
    }

private:
    struct DetachedTask
    {
        struct promise_type
        {
            [[nodiscard]] std::suspend_never initial_suspend() const noexcept
            {
                return {};
            }

            [[nodiscard]] std::suspend_never final_suspend() const noexcept
            {
                return {};
            }

            void unhandled_exception() const noexcept
            {
                std::terminate();
            }

            [[nodiscard]] DetachedTask get_return_object() const noexcept
            {
                return {};
            }

            void return_void() const noexcept {}
        };
    };

    void OnWorkStarted() noexcept
    {
        // Zero means Join() has run: nothing would resume a joiner for this work.
        if (m_count.fetch_add(1U, std::memory_order_relaxed) == 0U)
        {
            std::terminate();
        }
    }

    void OnWorkFinished() noexcept
    {
        if (m_count.fetch_sub(1U, std::memory_order_acq_rel) == 1U)
        {
            // Last access to the scope: the joiner may destroy it once resumed.
            m_continuation.resume();
        }
    }

    std::atomic_size_t m_count{1U};
    std::coroutine_handle<> m_continuation;
};

/// Moves an awaitable onto a scheduler: the returned task first awaits
/// `scheduler.Schedule()`, then the awaitable, so it runs where the scheduler runs.
template<typename Scheduler, Awaitable A>
Task<detail::StoredResult<typename AwaitableTraits<A>::AwaitResult>> ScheduleOn(Scheduler& scheduler, A operation)
{
    co_await scheduler.Schedule();
    co_return co_await std::move(operation);
}

} // namespace hwlib::execution
