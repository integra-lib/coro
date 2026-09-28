#include "test_support.hpp"

#include <gtest/gtest.h>

#include <hwlib/execution/coro/sync_wait.hpp>
#include <hwlib/execution/coro/task.hpp>
#include <hwlib/execution/coro/when_all_ready.hpp>
#include <stdexcept>
#include <tuple>

namespace
{

using hwlib::execution::SyncWait;
using hwlib::execution::Task;
using hwlib::execution::WhenAllReady;
using hwlib::execution::test::InlineEvent;
using hwlib::execution::test::Suspender;
using hwlib::execution::test::Tracked;

Task<int> Value(int value)
{
    co_return value;
}

Task<int> WaitThenReturn(Suspender& suspender, int value)
{
    co_await suspender;
    co_return value;
}

Task<int> Hold(Tracked /*unused*/)
{
    co_return 0;
}

TEST(WhenAllReady, NothingIsReadyAtOnce)
{
    auto body = []() -> Task<std::size_t> {
        auto results = co_await WhenAllReady();
        co_return std::tuple_size_v<decltype(results)>;
    };
    InlineEvent event;
    EXPECT_EQ(SyncWait(body(), event), 0U);
}

TEST(WhenAllReady, CollectsEveryResult)
{
    auto body = []() -> Task<int> {
        auto [a, b, c] = co_await WhenAllReady(Value(1), Value(20), []() -> Task<> { co_return; }());
        c.Result();
        co_return a.Result() + b.Result();
    };
    InlineEvent event;
    EXPECT_EQ(SyncWait(body(), event), 21);
}

TEST(WhenAllReady, ResumesOnceTheLastFinishes)
{
    Suspender first;
    Suspender second;
    int resumed = 0;
    int sum     = 0;
    auto body   = [&]() -> Task<> {
        auto [a, b] = co_await WhenAllReady(WaitThenReturn(first, 3), WaitThenReturn(second, 4));
        ++resumed;
        sum = a.Result() + b.Result();
    };
    auto outer = body();
    outer.Resume();
    ASSERT_TRUE(first.IsWaiting());
    ASSERT_TRUE(second.IsWaiting());
    second.Resume();
    EXPECT_EQ(resumed, 0);
    first.Resume();
    EXPECT_EQ(resumed, 1);
    EXPECT_EQ(sum, 7);
}

TEST(WhenAllReady, OneTaskThatSuspends)
{
    Suspender only;
    int resumed = 0;
    auto body   = [&]() -> Task<> {
        std::ignore = co_await WhenAllReady(WaitThenReturn(only, 1));
        ++resumed;
    };
    auto outer = body();
    outer.Resume();
    EXPECT_EQ(resumed, 0);
    only.Resume();
    EXPECT_EQ(resumed, 1);
}

int g_shared = 3;

TEST(WhenAllReady, AReferenceResultIsTheReferent)
{
    auto body = []() -> Task<bool> {
        auto [ref] = co_await WhenAllReady([]() -> Task<int&> { co_return g_shared; }());
        co_return &ref.Result() == &g_shared;
    };
    InlineEvent event;
    EXPECT_TRUE(SyncWait(body(), event));
}

TEST(WhenAllReady, AwaitingItAgainHandsTheResultsBack)
{
    auto body = []() -> Task<int> {
        auto all        = WhenAllReady(Value(2), Value(5));
        auto& [a, b]    = co_await all;
        const int first = a.Result() + b.Result();
        auto& [c, d]    = co_await all; // already done: does not start them again
        co_return first * 10 + c.Result() + d.Result();
    };
    InlineEvent event;
    EXPECT_EQ(SyncWait(body(), event), 77);
}

TEST(WhenAllReady, MoveAssigningAResultDestroysTheFrameItHeld)
{
    // a139's WhenAllTask overwrote its handle on move assignment and leaked the
    // coroutine frame it held.
    Tracked::alive = 0;
    auto body      = []() -> Task<int> {
        auto [x, y]      = co_await WhenAllReady(Hold(Tracked{}), Hold(Tracked{}));
        const int before = Tracked::alive;
        x                = std::move(y);
        co_return before * 10 + Tracked::alive;
    };
    InlineEvent event;
    EXPECT_EQ(SyncWait(body(), event), 21);
    EXPECT_EQ(Tracked::alive, 0);
}

#if defined(__cpp_exceptions)
Task<int> Throws()
{
    throw std::runtime_error{"one failed"};
    co_return 0;
}

TEST(WhenAllReady, AnExceptionStaysWithItsTask)
{
    auto body = []() -> Task<int> {
        auto [good, bad] = co_await WhenAllReady(Value(5), Throws());
        try
        {
            std::ignore = bad.Result();
        }
        catch (const std::runtime_error&)
        {
            co_return good.Result();
        }
        co_return -1;
    };
    InlineEvent event;
    EXPECT_EQ(SyncWait(body(), event), 5);
}
#endif

} // namespace
