#pragma once

#include <atomic>
#include <coroutine>
#include <exception>
#include <semaphore>
#include <thread>
#include <utility>

namespace hwlib::execution::test
{

/// The calling thread's id, read through a call the optimiser cannot see into.
/// clang at -O1 and above reuses std::this_thread::get_id() across a co_await,
/// although the coroutine may have moved to another thread there.
std::thread::id CurrentThreadId() noexcept;

/// A point where a coroutine stops until the test resumes it, from any thread.
class Suspender
{
public:
    [[nodiscard]] auto operator co_await() noexcept
    {
        struct Awaiter
        {
            Suspender& suspender;

            [[nodiscard]] bool await_ready() const noexcept
            {
                return false;
            }

            void await_suspend(std::coroutine_handle<> waiting) noexcept
            {
                suspender.m_waiting.store(waiting, std::memory_order_release);
            }

            void await_resume() const noexcept {}
        };

        return Awaiter{*this};
    }

    [[nodiscard]] bool IsWaiting() const noexcept
    {
        return static_cast<bool>(m_waiting.load(std::memory_order_acquire));
    }

    void Resume()
    {
        m_waiting.exchange(nullptr, std::memory_order_acq_rel).resume();
    }

private:
    // Atomic: a test may poll it from the thread that will resume the coroutine.
    std::atomic<std::coroutine_handle<>> m_waiting{nullptr};
};

/// Counts live instances: a coroutine frame holding one is destroyed exactly when
/// the count drops.
struct Tracked
{
    static inline int alive = 0;

    Tracked() noexcept
    {
        ++alive;
    }

    Tracked(const Tracked&) noexcept
    {
        ++alive;
    }

    Tracked(Tracked&&) noexcept
    {
        ++alive;
    }

    Tracked& operator=(const Tracked&) = default;
    Tracked& operator=(Tracked&&)      = default;

    ~Tracked()
    {
        --alive;
    }
};

/// For awaitables that complete in the calling thread.
struct InlineEvent
{
    bool set{false};

    void Set() noexcept
    {
        set = true;
    }

    void Wait() const noexcept
    {
        if (!set)
        {
            std::terminate(); // would block forever
        }
    }
};

/// For awaitables completed from another thread.
struct ThreadEvent
{
    std::binary_semaphore semaphore{0};

    void Set() noexcept
    {
        semaphore.release();
    }

    void Wait() noexcept
    {
        semaphore.acquire();
    }
};

} // namespace hwlib::execution::test
