// The examples in README.md, compiled and run: if one stops building or stops
// doing what the README says, this fails.

#include "test_support.hpp"

#include <gtest/gtest.h>

#include <array>
#include <atomic>
#include <chrono>
#include <hwlib/execution/coro.hpp>
#include <optional>
#include <semaphore>
#include <thread>
#include <vector>

namespace
{

using namespace hwlib::execution;
using namespace std::chrono_literals;

/// SyncWait()'s event: a binary semaphore behind Set() and Wait().
struct BinaryEvent
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

using Io = IoService<std::binary_semaphore>;

/// A thread running an IoService until destroyed.
class IoThread
{
public:
    explicit IoThread(Io& io)
        : m_io{io}
        , m_thread{[this](std::stop_token stop) {
            const std::stop_callback wakeOnStop{stop, [this] { m_io.Wake(); }};
            m_io.Run([&stop] { return stop.stop_requested(); }, 100ms);
        }}
    {}

private:
    Io& m_io;
    std::jthread m_thread;
};

// --- 1. Tasks: values, chaining, errors ---------------------------------------------

Task<int> ReadRaw()
{
    co_return 1234;
}

Task<float> ReadVolts()
{
    const int raw = co_await ReadRaw(); // runs ReadRaw() to its end, here
    co_return static_cast<float>(raw) * 0.001F;
}

TEST(ReadmeExamples, Tasks)
{
    BinaryEvent done;
    EXPECT_FLOAT_EQ(SyncWait(ReadVolts(), done), 1.234F);
}

// --- 2. A periodic job on the service thread -----------------------------------------

Task<> Blink(Io& io, int& toggles, int times)
{
    for (int i = 0; i < times; ++i)
    {
        ++toggles; // Toggle(led);
        co_await io.ScheduleAfter(5ms);
    }
}

TEST(ReadmeExamples, PeriodicJob)
{
    Io io;
    int toggles = 0;
    BinaryEvent done; // outlives the thread that sets it
    {
        IoThread thread{io};
        SyncWait(ScheduleOn(io, Blink(io, toggles, 4)), done);
    }
    EXPECT_EQ(toggles, 4);
}

// --- 3. Waiting for several things at once -------------------------------------------

Task<int> ReadTemperature(Io& io)
{
    co_await io.ScheduleAfter(10ms); // conversion time
    co_return 21;
}

Task<int> ReadHumidity(Io& io)
{
    co_await io.ScheduleAfter(10ms);
    co_return 45;
}

Task<int> ReadBoth(Io& io)
{
    // Both start now and wait in parallel: about 10 ms, not 20.
    auto [temperature, humidity] = co_await WhenAllReady(ReadTemperature(io), ReadHumidity(io));
    co_return temperature.Result() * 100 + humidity.Result();
}

TEST(ReadmeExamples, WaitForSeveral)
{
    Io io;
    BinaryEvent done;
    int both = 0;
    {
        IoThread thread{io};
        both = SyncWait(ScheduleOn(io, ReadBoth(io)), done);
    }
    EXPECT_EQ(both, 2145);
}

// --- 4. Fire and forget, then wait for all of it -------------------------------------

Task<> Handle(Io& io, int request, std::atomic<int>& handled)
{
    co_await io.ScheduleAfter(std::chrono::milliseconds{request});
    ++handled;
}

Task<> HandleAll(Io& io, std::atomic<int>& handled)
{
    AsyncScope scope;
    for (int request = 1; request <= 5; ++request)
    {
        scope.Spawn(Handle(io, request, handled)); // started now, not awaited
    }
    co_await scope.Join(); // before the scope goes away
}

TEST(ReadmeExamples, FireAndForget)
{
    Io io;
    BinaryEvent done;
    std::atomic<int> handled{0};
    {
        IoThread thread{io};
        SyncWait(ScheduleOn(io, HandleAll(io, handled)), done);
    }
    EXPECT_EQ(handled.load(), 5);
}

// --- 5. A stream of values -----------------------------------------------------------

AsyncGenerator<int> Samples(Io& io, int count)
{
    for (int i = 0; i < count; ++i)
    {
        co_await io.ScheduleAfter(2ms); // wait for the next sample
        co_yield i * 10;
    }
}

Task<int> Average(Io& io)
{
    int sum      = 0;
    int count    = 0;
    auto samples = Samples(io, 4);
    for (auto it = co_await samples.Begin(); it != samples.End();)
    {
        sum += *it;
        ++count;
        co_await ++it;
    }
    co_return count == 0 ? 0 : sum / count;
}

TEST(ReadmeExamples, Stream)
{
    Io io;
    BinaryEvent done;
    int average = 0;
    {
        IoThread thread{io};
        average = SyncWait(ScheduleOn(io, Average(io)), done);
    }
    EXPECT_EQ(average, 15);
}

// --- 6. Moving between threads -------------------------------------------------------

Task<std::array<std::thread::id, 3>> Hop(Io& fast, Io& slow)
{
    std::array<std::thread::id, 3> where{};
    co_await fast.Schedule();
    where[0] = test::CurrentThreadId(); // quick work on the fast thread
    co_await slow.Schedule();
    where[1] = test::CurrentThreadId(); // blocking work, e.g. a flash write
    co_await fast.Schedule();
    where[2] = test::CurrentThreadId(); // back
    co_return where;
}

TEST(ReadmeExamples, MovingBetweenThreads)
{
    Io fast;
    Io slow;
    BinaryEvent done;
    std::array<std::thread::id, 3> where{};
    {
        IoThread fastThread{fast};
        IoThread slowThread{slow};
        where = SyncWait(Hop(fast, slow), done);
    }
    EXPECT_EQ(where[0], where[2]);
    EXPECT_NE(where[0], where[1]);
}

// --- 7. From an interrupt to a coroutine ---------------------------------------------

MpmcQueue<std::uint16_t, 16> g_adcSamples;  // filled by the ADC interrupt
std::atomic<std::uint32_t> g_adcDropped{0}; // what it had to throw away

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
            co_return sum; // stopped and drained
        }
        else
        {
            co_await io.ScheduleAfter(1ms); // nothing yet: let the thread do other work
        }
    }
}

TEST(ReadmeExamples, FromAnInterrupt)
{
    Io io;
    BinaryEvent done;
    std::atomic<bool> adcStopped{false};
    std::atomic<int> accepted{0};
    int sum = 0;
    {
        IoThread thread{io};
        std::jthread interrupts{[&] {
            for (std::uint16_t sample = 1; sample <= 8; ++sample)
            {
                const std::uint32_t droppedBefore = g_adcDropped.load();
                AdcInterrupt(sample);
                if (g_adcDropped.load() == droppedBefore)
                {
                    accepted += sample;
                }
                std::this_thread::sleep_for(1ms);
            }
            adcStopped = true;
        }};
        sum = SyncWait(ScheduleOn(io, CollectSamples(io, adcStopped)), done);
    }
    EXPECT_EQ(sum, accepted.load()); // every accepted sample arrives exactly once
}

// --- 8. Errors ------------------------------------------------------------------------

#if defined(__cpp_exceptions)
Task<int> MayFail(bool fail)
{
    if (fail)
    {
        throw std::runtime_error{"sensor not answering"};
    }
    co_return 1;
}

Task<int> Tolerant()
{
    try
    {
        co_return co_await MayFail(true);
    }
    catch (const std::runtime_error&)
    {
        co_return -1; // the exception arrived where the task was awaited
    }
}

TEST(ReadmeExamples, Errors)
{
    BinaryEvent done;
    EXPECT_EQ(SyncWait(Tolerant(), done), -1);
}
#endif

} // namespace
