#pragma once

#include <atomic>
#include <chrono>
#include <concepts>
#include <coroutine>
#include <cstddef>

namespace hwlib::execution
{

/// What IoService wakes its thread with: std::binary_semaphore fits, and so does
/// a two-line wrapper over an RTOS semaphore. It is released at most once between
/// two acquisitions, so a binary one is enough.
template<typename S, typename Clock>
concept WakeSemaphore = requires(S& semaphore, typename Clock::time_point until) {
    semaphore.release();
    {
        semaphore.try_acquire_until(until)
    } -> std::convertible_to<bool>;
};

/// A scheduler for coroutines, run by one thread: `co_await io.Schedule()` moves
/// the awaiting coroutine onto that thread, `co_await io.ScheduleAfter(1s)` does so
/// once the time has passed. Scheduling works from any thread; Run(), ProcessOne()
/// and ProcessTimers() only from the one that runs the service.
///
/// Nothing is allocated: each scheduled coroutine is a node of an intrusive list,
/// in its own frame. Pushes go onto a lock-free stack that the running thread takes
/// whole and turns back into arrival order, so there is no capacity to overflow —
/// a139 lost every coroutine but the last of those it could not fit into its
/// 32-slot queue — and no reordering when there are many.
///
/// `Clock` must be steady, and a clock that says otherwise does not compile: a139
/// used high_resolution_clock, which libstdc++ defines as the system clock, so
/// setting the time moved every timer.
template<typename Semaphore, typename Clock = std::chrono::steady_clock>
    requires std::chrono::is_clock_v<Clock> && Clock::is_steady && WakeSemaphore<Semaphore, Clock>
class IoService
{
public:
    using TimePoint = typename Clock::time_point;
    using Duration  = typename Clock::duration;

    class ScheduleOperation
    {
    public:
        explicit ScheduleOperation(IoService& service, TimePoint resumeTime = {}) noexcept
            : m_service{service}
            , m_resumeTime{resumeTime}
        {}

        // A node of the service's lists while scheduled: never copied or moved.
        // Schedule() returns it as a prvalue, awaited in place.
        ScheduleOperation(const ScheduleOperation&)            = delete;
        ScheduleOperation& operator=(const ScheduleOperation&) = delete;
        ScheduleOperation(ScheduleOperation&&)                 = delete;
        ScheduleOperation& operator=(ScheduleOperation&&)      = delete;
        ~ScheduleOperation()                                   = default;

        [[nodiscard]] bool await_ready() const noexcept
        {
            return false;
        }

        void await_suspend(std::coroutine_handle<> awaiter) noexcept
        {
            // The coroutine counts as suspended before this runs ([expr.await]), so
            // the service thread may resume it — and its frame, holding this
            // operation, may be gone — as soon as Enqueue() has published it.
            // Nothing here touches the operation after that; Enqueue() only the
            // service.
            m_handle           = awaiter;
            IoService& service = m_service;
            const bool delayed = Clock::now() < m_resumeTime;
            service.Enqueue(this, delayed);
        }

        void await_resume() const noexcept {}

    private:
        friend class IoService;

        IoService& m_service;
        TimePoint m_resumeTime;
        std::coroutine_handle<> m_handle;
        ScheduleOperation* m_next{nullptr};
    };

    IoService()                            = default;
    IoService(const IoService&)            = delete;
    IoService& operator=(const IoService&) = delete;
    IoService(IoService&&)                 = delete;
    IoService& operator=(IoService&&)      = delete;
    ~IoService()                           = default;

    [[nodiscard]] ScheduleOperation Schedule() noexcept
    {
        return ScheduleOperation{*this};
    }

    [[nodiscard]] ScheduleOperation ScheduleAt(TimePoint time) noexcept
    {
        return ScheduleOperation{*this, time};
    }

    template<typename Rep, typename Period>
    [[nodiscard]] ScheduleOperation ScheduleAfter(std::chrono::duration<Rep, Period> delay) noexcept
    {
        return ScheduleOperation{*this, Clock::now() + std::chrono::duration_cast<Duration>(delay)};
    }

    /// Resumes the oldest ready coroutine; false when none is ready.
    bool ProcessOne()
    {
        if (m_readyHead == nullptr)
        {
            AppendReady(TakeInOrder(m_readyStack));
        }
        ScheduleOperation* const operation = PopReady();
        if (operation == nullptr)
        {
            return false;
        }
        // The operation lives in the coroutine's frame: not touched after this.
        operation->m_handle.resume();
        return true;
    }

    /// Makes the timers that are due ready; returns when the next one is, or
    /// TimePoint::max() when none is left.
    TimePoint ProcessTimers()
    {
        for (ScheduleOperation* operation = TakeInOrder(m_timerStack); operation != nullptr;)
        {
            ScheduleOperation* const next = operation->m_next;
            InsertTimer(operation);
            operation = next;
        }
        const TimePoint now = Clock::now();
        while (m_timers != nullptr && m_timers->m_resumeTime <= now)
        {
            ScheduleOperation* const due = m_timers;
            m_timers                     = due->m_next;
            due->m_next                  = nullptr;
            AppendReady(due);
        }
        return m_timers != nullptr ? m_timers->m_resumeTime : TimePoint::max();
    }

    /// Runs until `stopRequested()` says so, in rounds: makes due timers ready,
    /// resumes what is ready when the round starts, then sleeps until the next
    /// timer, something is scheduled, Wake() — or `maxSleep` has passed.
    ///
    /// What is scheduled during a round — a coroutine rescheduling itself, another
    /// thread flooding the service — waits for the next one, so timers and the stop
    /// request are looked at every round however busy the service is.
    template<std::predicate StopRequested>
    void Run(StopRequested stopRequested, Duration maxSleep)
    {
        while (!stopRequested())
        {
            const TimePoint deadline = ProcessTimers();
            AppendReady(TakeInOrder(m_readyStack));
            for (ScheduleOperation* const last = m_readyTail; last != nullptr;)
            {
                ScheduleOperation* const operation = PopReady();
                const bool endOfRound              = operation == last;
                operation->m_handle.resume(); // not touched after this
                if (endOfRound)
                {
                    break;
                }
            }
            const TimePoint now    = Clock::now();
            const TimePoint wakeAt = deadline - now < maxSleep ? deadline : now + maxSleep;
            if (m_wake.try_acquire_until(wakeAt))
            {
                // Reset only after the release was consumed, so the semaphore is
                // never released twice without an acquire in between.
                m_wakePending.store(false, std::memory_order_release);
            }
        }
    }

    /// Makes Run() go round now — after changing what `stopRequested` returns.
    void Wake() noexcept
    {
        if (!m_wakePending.exchange(true, std::memory_order_acq_rel))
        {
            m_wake.release();
        }
    }

private:
    void Enqueue(ScheduleOperation* operation, bool delayed) noexcept
    {
        std::atomic<ScheduleOperation*>& stack = delayed ? m_timerStack : m_readyStack;
        ScheduleOperation* head                = stack.load(std::memory_order_relaxed);
        do
        {
            operation->m_next = head;
        } while (!stack.compare_exchange_weak(head, operation, std::memory_order_release, std::memory_order_relaxed));
        Wake();
    }

    [[nodiscard]] ScheduleOperation* PopReady() noexcept
    {
        ScheduleOperation* const operation = m_readyHead;
        if (operation != nullptr)
        {
            m_readyHead = operation->m_next;
            if (m_readyHead == nullptr)
            {
                m_readyTail = nullptr;
            }
        }
        return operation;
    }

    /// Takes a whole stack and reverses it into the order it was pushed in.
    [[nodiscard]] static ScheduleOperation* TakeInOrder(std::atomic<ScheduleOperation*>& stack) noexcept
    {
        ScheduleOperation* pushed  = stack.exchange(nullptr, std::memory_order_acquire);
        ScheduleOperation* ordered = nullptr;
        while (pushed != nullptr)
        {
            ScheduleOperation* const next = pushed->m_next;
            pushed->m_next                = ordered;
            ordered                       = pushed;
            pushed                        = next;
        }
        return ordered;
    }

    /// Appends a list, in order, to the ready list.
    void AppendReady(ScheduleOperation* list) noexcept
    {
        if (list == nullptr)
        {
            return;
        }
        if (m_readyTail == nullptr)
        {
            m_readyHead = list;
        }
        else
        {
            m_readyTail->m_next = list;
        }
        m_readyTail = list;
        while (m_readyTail->m_next != nullptr)
        {
            m_readyTail = m_readyTail->m_next;
        }
    }

    /// Sorted by resume time; after the timers due at the same time.
    void InsertTimer(ScheduleOperation* operation) noexcept
    {
        ScheduleOperation** link = &m_timers;
        while (*link != nullptr && (*link)->m_resumeTime <= operation->m_resumeTime)
        {
            link = &(*link)->m_next;
        }
        operation->m_next = *link;
        *link             = operation;
    }

    std::atomic<ScheduleOperation*> m_readyStack{nullptr};
    std::atomic<ScheduleOperation*> m_timerStack{nullptr};
    std::atomic<bool> m_wakePending{false};
    Semaphore m_wake{0};

    // Only the running thread touches these.
    ScheduleOperation* m_readyHead{nullptr};
    ScheduleOperation* m_readyTail{nullptr};
    ScheduleOperation* m_timers{nullptr};
};

} // namespace hwlib::execution
