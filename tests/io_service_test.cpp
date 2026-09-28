#include "test_support.hpp"

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <hwlib/execution/coro/async_scope.hpp>
#include <hwlib/execution/coro/io_service.hpp>
#include <hwlib/execution/coro/sync_wait.hpp>
#include <hwlib/execution/coro/task.hpp>
#include <semaphore>
#include <thread>
#include <vector>

namespace
{

using hwlib::execution::IoService;
using hwlib::execution::ScheduleOn;
using hwlib::execution::SyncWait;
using hwlib::execution::Task;
using hwlib::execution::test::ThreadEvent;
using namespace std::chrono_literals;

/// Time the test moves by hand.
struct FakeClock
{
    using rep                                        = std::int64_t;
    using period                                     = std::milli;
    using duration                                   = std::chrono::duration<rep, period>;
    using time_point                                 = std::chrono::time_point<FakeClock>;
    [[maybe_unused]] static constexpr bool is_steady = true;

    static inline time_point current{};

    static time_point now() noexcept
    {
        return current;
    }
};

/// Never blocks; counts, and fails the test if it is released twice without an
/// acquire in between — UB for a binary semaphore, and what a139's scheduler did
/// once more than 1024 operations were pending.
struct FakeSemaphore
{
    static inline int maxCount = 0;
    static inline int releases = 0;
    int count{0};

    explicit FakeSemaphore(std::ptrdiff_t initial) noexcept
        : count{static_cast<int>(initial)}
    {}

    void release() noexcept
    {
        ++count;
        ++releases;
        maxCount = std::max(maxCount, count);
    }

    bool try_acquire_until(FakeClock::time_point /*until*/) noexcept
    {
        if (count > 0)
        {
            --count;
            return true;
        }
        return false;
    }
};

using FakeService = IoService<FakeSemaphore, FakeClock>;

class IoServiceTest : public ::testing::Test
{
protected:
    void SetUp() override
    {
        FakeClock::current      = FakeClock::time_point{};
        FakeSemaphore::maxCount = 0;
        FakeSemaphore::releases = 0;
    }

    FakeService m_io;
};

Task<> Record(FakeService& io, std::vector<int>& order, int id)
{
    co_await io.Schedule();
    order.push_back(id);
}

Task<> RecordAfter(FakeService& io, std::vector<int>& order, int id, FakeClock::duration delay)
{
    co_await io.ScheduleAfter(delay);
    order.push_back(id);
}

TEST_F(IoServiceTest, NothingScheduledNothingToProcess)
{
    EXPECT_FALSE(m_io.ProcessOne());
    EXPECT_EQ(m_io.ProcessTimers(), FakeClock::time_point::max());
}

TEST_F(IoServiceTest, ScheduledCoroutinesRunOnProcessOneInArrivalOrder)
{
    // Far more than a139's 32-slot queue: past it, a139 put the overflow back on
    // its list by its tail and lost the rest.
    constexpr int COUNT = 100;
    std::vector<int> order;
    std::vector<Task<>> tasks;
    for (int i = 0; i < COUNT; ++i)
    {
        tasks.push_back(Record(m_io, order, i));
        tasks.back().Resume();
    }
    EXPECT_TRUE(order.empty());
    while (m_io.ProcessOne())
    {}
    ASSERT_EQ(order.size(), static_cast<std::size_t>(COUNT));
    for (int i = 0; i < COUNT; ++i)
    {
        EXPECT_EQ(order[static_cast<std::size_t>(i)], i);
    }
}

TEST_F(IoServiceTest, ATimerWaitsForTheClock)
{
    std::vector<int> order;
    auto task = RecordAfter(m_io, order, 1, 10ms);
    task.Resume();
    EXPECT_EQ(m_io.ProcessTimers(), FakeClock::time_point{10ms});
    EXPECT_FALSE(m_io.ProcessOne());
    FakeClock::current = FakeClock::time_point{9ms};
    EXPECT_EQ(m_io.ProcessTimers(), FakeClock::time_point{10ms});
    EXPECT_FALSE(m_io.ProcessOne());
    FakeClock::current = FakeClock::time_point{10ms};
    EXPECT_EQ(m_io.ProcessTimers(), FakeClock::time_point::max());
    EXPECT_TRUE(m_io.ProcessOne());
    EXPECT_EQ(order, (std::vector<int>{1}));
}

TEST_F(IoServiceTest, TimersRunByDeadlineAndInArrivalOrderWhenTied)
{
    std::vector<int> order;
    std::vector<Task<>> tasks;
    tasks.push_back(RecordAfter(m_io, order, 1, 30ms));
    tasks.push_back(RecordAfter(m_io, order, 2, 10ms));
    tasks.push_back(RecordAfter(m_io, order, 3, 10ms));
    tasks.push_back(RecordAfter(m_io, order, 4, 20ms));
    for (auto& task : tasks)
    {
        task.Resume();
    }
    FakeClock::current = FakeClock::time_point{30ms};
    EXPECT_EQ(m_io.ProcessTimers(), FakeClock::time_point::max());
    while (m_io.ProcessOne())
    {}
    EXPECT_EQ(order, (std::vector<int>{2, 3, 4, 1}));
}

TEST_F(IoServiceTest, TimersFallingDueJoinTheBackOfAQueueStillWaiting)
{
    std::vector<int> order;
    std::vector<Task<>> tasks;
    for (int id = 1; id <= 3; ++id)
    {
        tasks.push_back(Record(m_io, order, id));
        tasks.back().Resume();
    }
    for (int id = 4; id <= 5; ++id)
    {
        tasks.push_back(RecordAfter(m_io, order, id, 10ms));
        tasks.back().Resume();
    }
    EXPECT_TRUE(m_io.ProcessOne()); // 1; 2 and 3 still waiting
    FakeClock::current = FakeClock::time_point{10ms};
    std::ignore        = m_io.ProcessTimers(); // 4 and 5 join behind 3
    tasks.push_back(Record(m_io, order, 6));
    tasks.back().Resume();
    while (m_io.ProcessOne())
    {}
    EXPECT_EQ(order, (std::vector<int>{1, 2, 3, 4, 5, 6}));
}

TEST_F(IoServiceTest, ATimeInThePastIsReadyAtOnce)
{
    FakeClock::current = FakeClock::time_point{100ms};
    std::vector<int> order;
    auto body = [&]() -> Task<> {
        co_await m_io.ScheduleAt(FakeClock::time_point{50ms});
        order.push_back(1);
    };
    auto task = body();
    task.Resume();
    EXPECT_TRUE(m_io.ProcessOne());
    EXPECT_EQ(order, (std::vector<int>{1}));
}

TEST_F(IoServiceTest, WakesAreCoalescedIntoOneRelease)
{
    std::vector<int> order;
    std::vector<Task<>> tasks;
    for (int round = 0; round < 3; ++round)
    {
        for (int i = 0; i < 2000; ++i)
        {
            tasks.push_back(Record(m_io, order, i));
            tasks.back().Resume();
        }
        int rounds = 0;
        m_io.Run([&] { return rounds++ == 2; }, 1ms);
    }
    EXPECT_EQ(order.size(), 6000U);
    EXPECT_EQ(FakeSemaphore::maxCount, 1);
    EXPECT_EQ(FakeSemaphore::releases, 3);
}

TEST_F(IoServiceTest, RunGetsRoundToTimersAndStopUnderContinuousLoad)
{
    // A coroutine that reschedules itself forever: with the ready queue drained to
    // empty each round, Run() never came back to its timers or its stop request.
    int spins    = 0;
    auto spinner = [&]() -> Task<> {
        for (;;)
        {
            co_await m_io.Schedule();
            ++spins;
        }
    };
    std::vector<int> order;
    auto spin  = spinner();
    auto timer = RecordAfter(m_io, order, 1, 10ms);
    spin.Resume();
    timer.Resume();
    FakeClock::current = FakeClock::time_point{10ms};
    int rounds         = 0;
    m_io.Run([&] { return rounds++ == 3; }, 1ms);
    EXPECT_EQ(order, (std::vector<int>{1}));
    EXPECT_EQ(spins, 3); // one step per round
}

TEST_F(IoServiceTest, ScheduleOnAVoidTask)
{
    bool ran  = false;
    auto work = [&]() -> Task<> {
        ran = true;
        co_return;
    };
    auto moved = ScheduleOn(m_io, work());
    moved.Resume();
    EXPECT_FALSE(ran);
    EXPECT_TRUE(m_io.ProcessOne());
    EXPECT_TRUE(ran);
    EXPECT_TRUE(moved.IsReady());
}

TEST_F(IoServiceTest, ScheduleOnRunsTheWorkThere)
{
    std::vector<int> order;
    auto work = [&]() -> Task<int> {
        order.push_back(2);
        co_return 7;
    };
    auto moved = ScheduleOn(m_io, work());
    moved.Resume();
    EXPECT_TRUE(order.empty());
    EXPECT_TRUE(m_io.ProcessOne());
    EXPECT_EQ(order, (std::vector<int>{2}));
    EXPECT_EQ(moved.Result(), 7);
}

// --- A real thread ------------------------------------------------------------------

using ThreadService = IoService<std::binary_semaphore>;

class Runner
{
public:
    Runner()
        : m_thread{[this] { m_io.Run([this] { return m_stop.load(); }, 50ms); }}
    {}

    Runner(const Runner&)            = delete;
    Runner& operator=(const Runner&) = delete;
    Runner(Runner&&)                 = delete;
    Runner& operator=(Runner&&)      = delete;

    ~Runner()
    {
        m_stop = true;
        m_io.Wake();
        m_thread.join();
    }

    [[nodiscard]] ThreadService& Io() noexcept
    {
        return m_io;
    }

    [[nodiscard]] std::thread::id Id() const noexcept
    {
        return m_thread.get_id();
    }

private:
    ThreadService m_io;
    std::atomic<bool> m_stop{false};
    std::thread m_thread;
};

TEST(IoServiceThread, ScheduleMovesTheCoroutineToTheServiceThread)
{
    // Declared first: Set() may still be inside release() on the service thread
    // when SyncWait() returns, so the event must outlive that thread.
    ThreadEvent event;
    Runner runner;
    auto where = [](ThreadService& io) -> Task<std::thread::id> {
        co_await io.Schedule();
        co_return hwlib::execution::test::CurrentThreadId();
    };
    EXPECT_EQ(SyncWait(where(runner.Io()), event), runner.Id());
}

TEST(IoServiceThread, ScheduleAfterWaitsAtLeastTheDelay)
{
    // Declared first: Set() may still be inside release() on the service thread
    // when SyncWait() returns, so the event must outlive that thread.
    ThreadEvent event;
    Runner runner;
    auto sleep       = [](ThreadService& io) -> Task<> { co_await io.ScheduleAfter(30ms); };
    const auto start = std::chrono::steady_clock::now();
    SyncWait(sleep(runner.Io()), event);
    EXPECT_GE(std::chrono::steady_clock::now() - start, 30ms);
}

TEST(IoServiceThread, ManyThreadsScheduleAtOnce)
{
    constexpr int PER_THREAD = 2000;
    constexpr int THREADS    = 4;
    std::atomic<int> ran{0};
    auto work = [](ThreadService& io, std::atomic<int>& counter) -> Task<> {
        co_await io.Schedule();
        ++counter;
    };
    // The frames finish on the service thread; they are destroyed only after it
    // has been joined, not when the counter says they ran — a frame is still in
    // use between its last statement and its final suspension.
    std::vector<std::vector<Task<>>> tasks(THREADS);
    {
        Runner runner;
        std::vector<std::thread> producers;
        for (int t = 0; t < THREADS; ++t)
        {
            producers.emplace_back([&, t] {
                auto& mine = tasks[static_cast<std::size_t>(t)];
                mine.reserve(PER_THREAD);
                for (int i = 0; i < PER_THREAD; ++i)
                {
                    mine.push_back(work(runner.Io(), ran));
                    mine.back().Resume();
                }
            });
        }
        for (auto& producer : producers)
        {
            producer.join();
        }
        while (ran.load() < PER_THREAD * THREADS)
        {
            std::this_thread::sleep_for(1ms);
        }
    }
    EXPECT_EQ(ran.load(), PER_THREAD * THREADS);
}

} // namespace
