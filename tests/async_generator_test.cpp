#include "test_support.hpp"

#include <gtest/gtest.h>

#include <array>
#include <hwlib/execution/coro/async_generator.hpp>
#include <hwlib/execution/coro/sync_wait.hpp>
#include <hwlib/execution/coro/task.hpp>
#include <stdexcept>
#include <vector>

namespace
{

using hwlib::execution::AsyncGenerator;
using hwlib::execution::SyncWait;
using hwlib::execution::Task;
using hwlib::execution::test::InlineEvent;
using hwlib::execution::test::Suspender;
using hwlib::execution::test::Tracked;

AsyncGenerator<int> Count(int to)
{
    for (int i = 1; i <= to; ++i)
    {
        co_yield i;
    }
}

template<typename T>
Task<std::vector<T>> Collect(AsyncGenerator<T> generator)
{
    std::vector<T> values;
    for (auto it = co_await generator.Begin(); it != generator.End(); co_await ++it)
    {
        values.push_back(*it);
    }
    co_return values;
}

AsyncGenerator<int> Hold(Tracked /*unused*/)
{
    co_yield 1;
}

TEST(AsyncGenerator, YieldsInOrder)
{
    InlineEvent event;
    EXPECT_EQ(SyncWait(Collect(Count(3)), event), (std::vector<int>{1, 2, 3}));
}

TEST(AsyncGenerator, OneThatYieldsNothingIsEmpty)
{
    InlineEvent event;
    EXPECT_TRUE(SyncWait(Collect(Count(0)), event).empty());
}

TEST(AsyncGenerator, ReferencesReachTheYieldedObjects)
{
    std::array<int, 3> values{1, 2, 3};
    auto refs = [](std::array<int, 3>& array) -> AsyncGenerator<int&> {
        for (int& value : array)
        {
            co_yield value;
        }
    };
    auto body = [&]() -> Task<> {
        auto generator = refs(values);
        for (auto it = co_await generator.Begin(); it != generator.End(); co_await ++it)
        {
            *it *= 10;
        }
    };
    InlineEvent event;
    SyncWait(body(), event);
    EXPECT_EQ(values, (std::array<int, 3>{10, 20, 30}));
}

TEST(AsyncGenerator, TheGeneratorMayAwaitBetweenValues)
{
    Suspender suspender;
    auto slow = [](Suspender& between) -> AsyncGenerator<int> {
        co_yield 1;
        co_await between;
        co_yield 2;
    };
    std::vector<int> got;
    auto body = [&]() -> Task<> {
        auto generator = slow(suspender);
        for (auto it = co_await generator.Begin(); it != generator.End(); co_await ++it)
        {
            got.push_back(*it);
        }
    };
    auto consumer = body();
    consumer.Resume();
    EXPECT_EQ(got, (std::vector<int>{1}));
    suspender.Resume();
    EXPECT_EQ(got, (std::vector<int>{1, 2}));
    EXPECT_TRUE(consumer.IsReady());
}

TEST(AsyncGenerator, MoveAssignmentDestroysTheFrameItHeld)
{
    // a139's move assignment overwrote the handle and leaked the frame.
    Tracked::alive = 0;
    auto generator = Hold(Tracked{});
    EXPECT_EQ(Tracked::alive, 1);
    generator = Hold(Tracked{});
    EXPECT_EQ(Tracked::alive, 1);
}

#if defined(__cpp_exceptions)
AsyncGenerator<int> FailsAfterOne()
{
    co_yield 1;
    throw std::runtime_error{"generator failed"};
}

TEST(AsyncGenerator, AnExceptionReachesTheConsumer)
{
    InlineEvent event;
    EXPECT_THROW(std::ignore = SyncWait(Collect(FailsAfterOne()), event), std::runtime_error);
}
#endif

} // namespace
