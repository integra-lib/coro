#include "test_support.hpp"

#include <gtest/gtest.h>

#include <chrono>
#include <hwlib/execution/coro/sync_wait.hpp>
#include <hwlib/execution/coro/task.hpp>
#include <string>
#include <thread>

namespace
{

using hwlib::execution::SyncWait;
using hwlib::execution::Task;
using hwlib::execution::test::InlineEvent;
using hwlib::execution::test::Suspender;
using hwlib::execution::test::ThreadEvent;

std::string g_kept = "keep me";

Task<std::string&> KeptByReference()
{
    co_return g_kept;
}

TEST(SyncWait, ReturnsTheValue)
{
    InlineEvent event;
    EXPECT_EQ(SyncWait([]() -> Task<int> { co_return 4; }(), event), 4);
}

TEST(SyncWait, RunsAVoidTask)
{
    bool ran = false;
    InlineEvent event;
    SyncWait(
        [&]() -> Task<> {
            ran = true;
            co_return;
        }(),
        event);
    EXPECT_TRUE(ran);
}

TEST(SyncWait, AReferenceResultIsCopiedNotMovedFrom)
{
    // In a139 this did not compile: its result() returned the stored value as
    // `T&&` with T a reference.
    InlineEvent event;
    const std::string copy = SyncWait(KeptByReference(), event);
    EXPECT_EQ(copy, "keep me");
    EXPECT_EQ(g_kept, "keep me");
}

TEST(SyncWait, AcceptsATaskByLvalue)
{
    auto task = []() -> Task<int> { co_return 8; }();
    InlineEvent event;
    EXPECT_EQ(SyncWait(task, event), 8);
    EXPECT_TRUE(task.IsReady());
    EXPECT_EQ(task.Result(), 8); // an lvalue awaited is not emptied
}

TEST(SyncWait, BlocksUntilAnotherThreadCompletesTheAwaitable)
{
    Suspender suspender;
    auto task = [&]() -> Task<int> {
        co_await suspender;
        co_return 11;
    };
    ThreadEvent event;
    std::thread resumer{[&] {
        while (!suspender.IsWaiting())
        {
            std::this_thread::sleep_for(std::chrono::milliseconds{1});
        }
        suspender.Resume();
    }};
    EXPECT_EQ(SyncWait(task(), event), 11);
    resumer.join();
}

} // namespace
