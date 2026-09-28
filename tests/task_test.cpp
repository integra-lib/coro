#include "test_support.hpp"

#include <gtest/gtest.h>

#include <coroutine>
#include <hwlib/execution/coro/awaitable.hpp>
#include <hwlib/execution/coro/sync_wait.hpp>
#include <hwlib/execution/coro/task.hpp>
#include <memory>
#include <stdexcept>
#include <string>

namespace hwlib::execution
{
// Defined in task_second_unit.cpp: a second translation unit that includes task.hpp.
Task<int> SecondUnitTask();
} // namespace hwlib::execution

namespace
{

using hwlib::execution::SyncWait;
using hwlib::execution::Task;
using hwlib::execution::test::InlineEvent;
using hwlib::execution::test::Suspender;
using hwlib::execution::test::Tracked;

int g_referent = 7;

Task<int> Twenty()
{
    co_return 20;
}

Task<int> PlusOne()
{
    co_return (co_await Twenty()) + 1;
}

Task<int&> Referent()
{
    co_return g_referent;
}

Task<> SetFlag(bool& flag)
{
    flag = true;
    co_return;
}

Task<int> Hold(Tracked /*unused*/)
{
    co_return 0;
}

Task<int> WaitThenReturn(Suspender& suspender, int value)
{
    co_await suspender;
    co_return value;
}

TEST(Task, IsLazyUntilResumed)
{
    bool ran  = false;
    auto task = SetFlag(ran);
    EXPECT_FALSE(ran);
    EXPECT_FALSE(task.IsReady());
    task.Resume();
    EXPECT_TRUE(ran);
    EXPECT_TRUE(task.IsReady());
}

TEST(Task, ResultOfAResumedTask)
{
    auto task = Twenty();
    task.Resume();
    EXPECT_EQ(task.Result(), 20);
}

TEST(Task, AwaitingATaskChainsItsValue)
{
    InlineEvent event;
    EXPECT_EQ(SyncWait(PlusOne(), event), 21);
}

TEST(Task, AReferenceResultIsTheReferent)
{
    auto task = Referent();
    task.Resume();
    EXPECT_EQ(&task.Result(), &g_referent);
}

TEST(Task, AMoveOnlyResultIsMovedOut)
{
    auto make = []() -> Task<std::unique_ptr<int>> { co_return std::make_unique<int>(5); };
    InlineEvent event;
    const std::unique_ptr<int> value = SyncWait(make(), event);
    ASSERT_NE(value, nullptr);
    EXPECT_EQ(*value, 5);
}

TEST(Task, TheAwaiterResumesWhenTheTaskFinishes)
{
    Suspender suspender;
    int got = 0;
    // A capturing coroutine lambda must outlive its frame: kept in a variable.
    auto body  = [&]() -> Task<> { got = co_await WaitThenReturn(suspender, 9); };
    auto outer = body();
    outer.Resume();
    EXPECT_EQ(got, 0);
    ASSERT_TRUE(suspender.IsWaiting());
    suspender.Resume();
    EXPECT_EQ(got, 9);
    EXPECT_TRUE(outer.IsReady());
}

TEST(Task, WhenReadyDoesNotTakeTheResult)
{
    Suspender suspender;
    bool ready = false;
    auto inner = WaitThenReturn(suspender, 3);
    auto body  = [&]() -> Task<> {
        co_await inner.WhenReady();
        ready = true;
    };
    auto outer = body();
    outer.Resume();
    EXPECT_FALSE(ready);
    suspender.Resume();
    EXPECT_TRUE(ready);
    EXPECT_EQ(inner.Result(), 3);
}

TEST(Task, MoveAssignmentDestroysTheFrameItHeld)
{
    Tracked::alive = 0;
    auto task      = Hold(Tracked{});
    EXPECT_EQ(Tracked::alive, 1);
    task = Hold(Tracked{});
    EXPECT_EQ(Tracked::alive, 1);
}

TEST(Task, DestroyingAnUnstartedTaskDestroysItsFrame)
{
    Tracked::alive = 0;
    {
        auto task = Hold(Tracked{});
        EXPECT_EQ(Tracked::alive, 1);
    }
    EXPECT_EQ(Tracked::alive, 0);
}

TEST(TaskDeathTest, ReadingAnUnfinishedResultEndsTheProgram)
{
    // A caller error, not a value: reading before the task finished.
    Suspender suspender;
    auto task = WaitThenReturn(suspender, 1);
    task.Resume();
    EXPECT_DEATH(std::ignore = task.Result(), "terminate called");
    suspender.Resume();
}

TEST(TaskDeathTest, ResumingAFinishedTaskEndsTheProgram)
{
    auto task = Twenty();
    task.Resume();
    ASSERT_TRUE(task.IsReady());
    EXPECT_DEATH(task.Resume(), "terminate called");
}

TEST(Task, ThisCoroutineIsTheCallersHandle)
{
    std::coroutine_handle<> seen;
    auto body = [&]() -> Task<> { seen = co_await hwlib::execution::ThisCoroutine(); };
    auto task = body();
    task.Resume();
    ASSERT_TRUE(seen);
    EXPECT_TRUE(seen.done()); // finished: the handle is the task's own frame
    EXPECT_TRUE(task.IsReady());
}

TEST(Task, WorksFromASecondTranslationUnit)
{
    // a139 defined TaskPromise<void>::get_return_object() in the header without
    // `inline`: two units including it did not link.
    InlineEvent event;
    EXPECT_EQ(SyncWait(hwlib::execution::SecondUnitTask(), event), 2);
}

#if defined(__cpp_exceptions)
Task<int> Throws()
{
    throw std::runtime_error{"boom"};
    co_return 0;
}

TEST(Task, AnExceptionReachesTheAwaiter)
{
    auto outer = []() -> Task<std::string> {
        try
        {
            co_return std::to_string(co_await Throws());
        }
        catch (const std::runtime_error& error)
        {
            co_return error.what();
        }
    };
    InlineEvent event;
    EXPECT_EQ(SyncWait(outer(), event), "boom");
}

TEST(Task, AnExceptionReachesSyncWait)
{
    InlineEvent event;
    EXPECT_THROW(std::ignore = SyncWait(Throws(), event), std::runtime_error);
}

TEST(Task, TheResultOfAMovedFromTaskIsABrokenPromise)
{
    auto moved = Twenty();
    auto keep  = std::move(moved);
    EXPECT_THROW(std::ignore = moved.Result(), hwlib::execution::BrokenPromise); // NOLINT(bugprone-use-after-move)
}

TEST(Task, AwaitingAMovedFromTaskIsABrokenPromise)
{
    auto moved = Twenty();
    auto keep  = std::move(moved);
    auto outer = [&]() -> Task<int> { co_return co_await moved; }; // NOLINT(bugprone-use-after-move)
    InlineEvent event;
    EXPECT_THROW(std::ignore = SyncWait(outer(), event), hwlib::execution::BrokenPromise);
}
#endif

} // namespace
