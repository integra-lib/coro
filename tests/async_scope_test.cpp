#include "test_support.hpp"

#include <gtest/gtest.h>

#include <hwlib/execution/coro/async_scope.hpp>
#include <hwlib/execution/coro/task.hpp>

namespace
{

using hwlib::execution::AsyncScope;
using hwlib::execution::Task;
using hwlib::execution::test::Suspender;

Task<> WaitOn(Suspender& suspender, int& finished)
{
    co_await suspender;
    ++finished;
}

Task<> Immediate(int& finished)
{
    ++finished;
    co_return;
}

struct Joiner
{
    int resumed{0};

    Task<> Join(AsyncScope& scope)
    {
        co_await scope.Join();
        ++resumed;
    }
};

TEST(AsyncScope, JoinWithNothingSpawnedDoesNotWait)
{
    AsyncScope scope;
    Joiner joiner;
    auto join = joiner.Join(scope);
    join.Resume();
    EXPECT_EQ(joiner.resumed, 1);
}

TEST(AsyncScope, SpawnStartsTheWorkAtOnce)
{
    AsyncScope scope;
    int finished = 0;
    scope.Spawn(Immediate(finished));
    EXPECT_EQ(finished, 1);
    Joiner joiner;
    auto join = joiner.Join(scope);
    join.Resume();
    EXPECT_EQ(joiner.resumed, 1);
}

TEST(AsyncScope, JoinWaitsForTheOneWorkStillRunning)
{
    // a139 started its count at 0 and kept cppcoro's `> 1` in Join(): with one work
    // running, Join() went straight on, and the work's end resumed it again.
    AsyncScope scope;
    Suspender suspender;
    int finished = 0;
    scope.Spawn(WaitOn(suspender, finished));
    Joiner joiner;
    auto join = joiner.Join(scope);
    join.Resume();
    EXPECT_EQ(joiner.resumed, 0);
    suspender.Resume();
    EXPECT_EQ(finished, 1);
    EXPECT_EQ(joiner.resumed, 1);
}

TEST(AsyncScope, JoinWaitsForTheLastOfSeveral)
{
    AsyncScope scope;
    Suspender first;
    Suspender second;
    Suspender third;
    int finished = 0;
    scope.Spawn(WaitOn(first, finished));
    scope.Spawn(WaitOn(second, finished));
    scope.Spawn(WaitOn(third, finished));
    Joiner joiner;
    auto join = joiner.Join(scope);
    join.Resume();
    second.Resume();
    first.Resume();
    EXPECT_EQ(joiner.resumed, 0);
    third.Resume();
    EXPECT_EQ(finished, 3);
    EXPECT_EQ(joiner.resumed, 1);
}

TEST(AsyncScope, WorkThatFinishedBeforeJoinIsNotWaitedFor)
{
    AsyncScope scope;
    Suspender suspender;
    int finished = 0;
    scope.Spawn(WaitOn(suspender, finished));
    suspender.Resume();
    Joiner joiner;
    auto join = joiner.Join(scope);
    join.Resume();
    EXPECT_EQ(joiner.resumed, 1);
}

TEST(AsyncScope, ASecondJoinReturnsAtOnce)
{
    AsyncScope scope;
    Joiner joiner;
    auto first = joiner.Join(scope);
    first.Resume();
    auto second = joiner.Join(scope);
    second.Resume();
    EXPECT_EQ(joiner.resumed, 2);
}

TEST(AsyncScopeDeathTest, SpawnAfterJoinEndsTheProgram)
{
    // Nothing would resume a joiner for work spawned after Join().
    AsyncScope scope;
    Joiner joiner;
    auto join = joiner.Join(scope);
    join.Resume();
    Suspender suspender;
    int finished = 0;
    EXPECT_DEATH(scope.Spawn(WaitOn(suspender, finished)), "terminate called");
}

TEST(AsyncScopeDeathTest, DestroyingItWithWorkRunningEndsTheProgram)
{
    EXPECT_DEATH(
        {
            Suspender suspender;
            int finished = 0;
            AsyncScope scope;
            scope.Spawn(WaitOn(suspender, finished));
        },
        "terminate called");
}

} // namespace
