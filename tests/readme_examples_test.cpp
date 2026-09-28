// The examples in README.md, compiled and run: if one stops building or stops
// doing what the README says, this fails.

#include "test_support.hpp"

#include <gtest/gtest.h>

#include <array>
#include <atomic>
#include <chrono>
#include <deque>
#include <hwlib/execution/coro.hpp>
#include <optional>
#include <semaphore>
#include <string>
#include <thread>
#include <utility>
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

// --- 9. A state machine next to a coroutine -----------------------------------------
//
// The same behaviour twice: once as a class polled from a main loop, once as a
// coroutine. Both run on one simulated clock, a millisecond per step, and must make
// the same calls at the same moments.

namespace sim
{

/// A steady clock that moves only when the test says so.
struct Clock
{
    using rep                       = std::int64_t;
    using period                    = std::milli;
    using duration                  = std::chrono::duration<rep, period>;
    using time_point                = std::chrono::time_point<Clock>;
    static constexpr bool is_steady = true;

    static inline duration current{0};

    static time_point now() noexcept
    {
        return time_point{current};
    }
};

/// The service is driven by ProcessTimers() and ProcessOne(), so nothing sleeps.
struct NoSleep
{
    explicit NoSleep(std::ptrdiff_t /*initial*/) noexcept {}

    void release() noexcept {}

    [[nodiscard]] bool try_acquire_until(Clock::time_point /*until*/) noexcept
    {
        return false;
    }
};

using Io = IoService<NoSleep, Clock>;

constexpr auto CONVERSION_TIME = 10ms;
constexpr auto RETRY_DELAY     = 20ms;
constexpr int MAX_ATTEMPTS     = 3;
constexpr auto POWER_UP_TIME   = 5ms;
constexpr int FLASHES          = 3;
constexpr auto ON_TIME         = 10ms;
constexpr auto OFF_TIME        = 10ms;
constexpr auto PAUSE           = 50ms;

/// What the fakes were asked to do, and when, on the simulated clock.
struct Recorder
{
    std::vector<std::string> events;
    std::vector<std::chrono::milliseconds> times;
    std::chrono::milliseconds now{0};

    void Note(std::string event)
    {
        events.push_back(std::move(event));
        times.push_back(now);
    }
};

/// A sensor that converts on command; scripted answers, then success.
struct Sensor
{
    Recorder& recorder;
    std::deque<bool> starts{};
    std::deque<std::optional<int>> reads{};

    bool StartConversion()
    {
        recorder.Note("start");
        const bool started = starts.empty() || starts.front();
        if (!starts.empty())
        {
            starts.pop_front();
        }
        return started;
    }

    std::optional<int> Read()
    {
        recorder.Note("read");
        std::optional<int> value{42};
        if (!reads.empty())
        {
            value = reads.front();
            reads.pop_front();
        }
        return value;
    }
};

struct Led
{
    Recorder& recorder;

    void On()
    {
        recorder.Note("on");
    }

    void Off()
    {
        recorder.Note("off");
    }
};

struct PowerSwitch
{
    Recorder& recorder;

    void On()
    {
        recorder.Note("power on");
    }

    void Off()
    {
        recorder.Note("power off");
    }
};

namespace machine
{

class Measurement
{
public:
    explicit Measurement(Sensor& sensor)
        : m_sensor{sensor}
    {}

    /// Called from the main loop, again and again; true once Result() is final.
    bool Poll(Clock::time_point now)
    {
        switch (m_state)
        {
        case State::eBackoff:
            if (now < m_deadline)
            {
                break;
            }
            [[fallthrough]];
        case State::eStart:
            if (m_sensor.StartConversion())
            {
                m_deadline = now + CONVERSION_TIME;
                m_state    = State::eConverting;
            }
            else
            {
                Retry(now);
            }
            break;
        case State::eConverting:
            if (now < m_deadline)
            {
                break;
            }
            m_result = m_sensor.Read();
            if (m_result.has_value())
            {
                m_state = State::eDone;
            }
            else
            {
                Retry(now);
            }
            break;
        case State::eDone: break;
        }
        return m_state == State::eDone;
    }

    [[nodiscard]] std::optional<int> Result() const
    {
        return m_result;
    }

private:
    enum class State : std::uint8_t
    {
        eStart,
        eConverting,
        eBackoff,
        eDone,
    };

    void Retry(Clock::time_point now)
    {
        m_deadline = now + RETRY_DELAY;
        m_state    = ++m_attempt < MAX_ATTEMPTS ? State::eBackoff : State::eDone;
    }

    Sensor& m_sensor;
    State m_state{State::eStart};
    int m_attempt{0};
    Clock::time_point m_deadline{};
    std::optional<int> m_result;
};

class BlinkPattern
{
public:
    explicit BlinkPattern(Led& led)
        : m_led{led}
    {}

    void Poll(Clock::time_point now)
    {
        if (now < m_next)
        {
            return;
        }
        switch (m_state)
        {
        case State::eDark:
            m_led.On();
            m_state = State::eLit;
            m_next  = now + ON_TIME;
            break;
        case State::eLit:
            m_led.Off();
            m_state = State::eDark;
            if (++m_flash == FLASHES)
            {
                m_flash = 0;
                m_next  = now + PAUSE;
            }
            else
            {
                m_next = now + OFF_TIME;
            }
            break;
        }
    }

private:
    enum class State : std::uint8_t
    {
        eDark,
        eLit,
    };

    Led& m_led;
    State m_state{State::eDark};
    int m_flash{0};
    Clock::time_point m_next{};
};

class PoweredMeasurement
{
public:
    PoweredMeasurement(Sensor& sensor, PowerSwitch& power)
        : m_measurement{sensor}
        , m_power{power}
    {}

    bool Poll(Clock::time_point now)
    {
        switch (m_state)
        {
        case State::eOff:
            m_power.On();
            m_deadline = now + POWER_UP_TIME;
            m_state    = State::ePoweringUp;
            break;
        case State::ePoweringUp:
            if (now < m_deadline)
            {
                break;
            }
            m_state = State::eMeasuring;
            [[fallthrough]];
        case State::eMeasuring:
            if (m_measurement.Poll(now))
            {
                m_power.Off();
                m_state = State::eDone;
            }
            break;
        case State::eDone: break;
        }
        return m_state == State::eDone;
    }

    [[nodiscard]] std::optional<int> Result() const
    {
        return m_measurement.Result();
    }

private:
    enum class State : std::uint8_t
    {
        eOff,
        ePoweringUp,
        eMeasuring,
        eDone,
    };

    Measurement m_measurement;
    PowerSwitch& m_power;
    State m_state{State::eOff};
    Clock::time_point m_deadline{};
};

} // namespace machine

namespace coroutine
{

Task<std::optional<int>> Measure(Io& io, Sensor& sensor)
{
    for (int attempt = 0; attempt < MAX_ATTEMPTS; ++attempt)
    {
        if (attempt > 0)
        {
            co_await io.ScheduleAfter(RETRY_DELAY);
        }
        if (sensor.StartConversion())
        {
            co_await io.ScheduleAfter(CONVERSION_TIME);
            if (auto value = sensor.Read(); value.has_value())
            {
                co_return value;
            }
        }
    }
    co_return std::nullopt;
}

Task<> BlinkPattern(Io& io, Led& led)
{
    for (;;)
    {
        for (int flash = 0; flash < FLASHES; ++flash)
        {
            if (flash > 0)
            {
                co_await io.ScheduleAfter(OFF_TIME);
            }
            led.On();
            co_await io.ScheduleAfter(ON_TIME);
            led.Off();
        }
        co_await io.ScheduleAfter(PAUSE);
    }
}

Task<std::optional<int>> MeasurePowered(Io& io, Sensor& sensor, PowerSwitch& power)
{
    power.On();
    co_await io.ScheduleAfter(POWER_UP_TIME);
    const auto value = co_await Measure(io, sensor);
    power.Off();
    co_return value;
}

} // namespace coroutine

/// Polls a machine every simulated millisecond until it is done.
template<typename Machine>
void PollUntilDone(Machine& machine, Recorder& recorder)
{
    for (recorder.now = 0ms; recorder.now < 1s; ++recorder.now)
    {
        if (machine.Poll(Clock::time_point{recorder.now}))
        {
            return;
        }
    }
    FAIL() << "the machine never finished";
}

/// Runs a task on the service, a simulated millisecond per step, until it is done
/// or `limit` has passed.
template<typename T>
void RunSimulated(Io& io, Task<T>& task, Recorder& recorder, std::chrono::milliseconds limit = 1s)
{
    task.Resume(); // queued on io by ScheduleOn()
    for (recorder.now = 0ms; recorder.now <= limit; ++recorder.now)
    {
        Clock::current = recorder.now;
        std::ignore    = io.ProcessTimers();
        while (io.ProcessOne())
        {}
        if (task.IsReady())
        {
            return;
        }
    }
}

using Events = std::vector<std::string>;
using Times  = std::vector<std::chrono::milliseconds>;

/// A failed start, then a failed read, then a value: all three attempts.
void ScriptRetries(Sensor& sensor)
{
    sensor.starts = {false, true, true};
    sensor.reads  = {std::nullopt, 42};
}

const Events RETRIED        = {"start", "start", "read", "start", "read"};
const Times RETRIED_AT      = {0ms, 20ms, 30ms, 50ms, 60ms};
const Events GAVE_UP        = {"start", "start", "start"};
const Times GAVE_UP_AT      = {0ms, 20ms, 40ms};
const Events THREE_CYCLES   = {"on",  "off", "on",  "off", "on",  "off", "on",  "off", "on",
                               "off", "on",  "off", "on",  "off", "on",  "off", "on",  "off"};
const Times THREE_CYCLES_AT = {0ms,   10ms,  20ms,  30ms,  40ms,  50ms,  100ms, 110ms, 120ms,
                               130ms, 140ms, 150ms, 200ms, 210ms, 220ms, 230ms, 240ms, 250ms};
const Events POWERED        = {"power on", "start", "read", "power off"};
const Times POWERED_AT      = {0ms, 5ms, 15ms, 15ms};

TEST(ReadmeStateMachines, MeasurementRetries)
{
    Recorder recorder;
    Sensor sensor{recorder};
    ScriptRetries(sensor);
    machine::Measurement measurement{sensor};
    PollUntilDone(measurement, recorder);
    EXPECT_EQ(measurement.Result(), std::optional{42});
    EXPECT_EQ(recorder.events, RETRIED);
    EXPECT_EQ(recorder.times, RETRIED_AT);
}

TEST(ReadmeStateMachines, MeasureRetries)
{
    Recorder recorder;
    Sensor sensor{recorder};
    ScriptRetries(sensor);
    Io io;
    auto task = ScheduleOn(io, coroutine::Measure(io, sensor));
    RunSimulated(io, task, recorder);
    ASSERT_TRUE(task.IsReady());
    EXPECT_EQ(task.Result(), std::optional{42});
    EXPECT_EQ(recorder.events, RETRIED);
    EXPECT_EQ(recorder.times, RETRIED_AT);
}

TEST(ReadmeStateMachines, MeasurementGivesUp)
{
    Recorder recorder;
    Sensor sensor{recorder};
    sensor.starts = {false, false, false};
    machine::Measurement measurement{sensor};
    PollUntilDone(measurement, recorder);
    EXPECT_EQ(measurement.Result(), std::nullopt);
    EXPECT_EQ(recorder.events, GAVE_UP);
    EXPECT_EQ(recorder.times, GAVE_UP_AT);
}

TEST(ReadmeStateMachines, MeasureGivesUp)
{
    Recorder recorder;
    Sensor sensor{recorder};
    sensor.starts = {false, false, false};
    Io io;
    auto task = ScheduleOn(io, coroutine::Measure(io, sensor));
    RunSimulated(io, task, recorder);
    ASSERT_TRUE(task.IsReady());
    EXPECT_EQ(task.Result(), std::nullopt);
    EXPECT_EQ(recorder.events, GAVE_UP);
    EXPECT_EQ(recorder.times, GAVE_UP_AT);
}

TEST(ReadmeStateMachines, BlinkPatternMachine)
{
    Recorder recorder;
    Led led{recorder};
    machine::BlinkPattern pattern{led};
    for (recorder.now = 0ms; recorder.now <= 250ms; ++recorder.now)
    {
        pattern.Poll(Clock::time_point{recorder.now});
    }
    EXPECT_EQ(recorder.events, THREE_CYCLES);
    EXPECT_EQ(recorder.times, THREE_CYCLES_AT);
}

TEST(ReadmeStateMachines, BlinkPatternCoroutine)
{
    Recorder recorder;
    Led led{recorder};
    Io io;
    auto task = ScheduleOn(io, coroutine::BlinkPattern(io, led));
    RunSimulated(io, task, recorder, 250ms); // endless: stopped by the limit
    EXPECT_EQ(recorder.events, THREE_CYCLES);
    EXPECT_EQ(recorder.times, THREE_CYCLES_AT);
}

TEST(ReadmeStateMachines, PoweredMeasurement)
{
    Recorder recorder;
    Sensor sensor{recorder};
    PowerSwitch power{recorder};
    machine::PoweredMeasurement measurement{sensor, power};
    PollUntilDone(measurement, recorder);
    EXPECT_EQ(measurement.Result(), std::optional{42});
    EXPECT_EQ(recorder.events, POWERED);
    EXPECT_EQ(recorder.times, POWERED_AT);
}

TEST(ReadmeStateMachines, MeasurePowered)
{
    Recorder recorder;
    Sensor sensor{recorder};
    PowerSwitch power{recorder};
    Io io;
    auto task = ScheduleOn(io, coroutine::MeasurePowered(io, sensor, power));
    RunSimulated(io, task, recorder);
    ASSERT_TRUE(task.IsReady());
    EXPECT_EQ(task.Result(), std::optional{42});
    EXPECT_EQ(recorder.events, POWERED);
    EXPECT_EQ(recorder.times, POWERED_AT);
}

} // namespace sim

} // namespace
