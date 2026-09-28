#include "test_support.hpp"

#include <gtest/gtest.h>

#include <atomic>
#include <hwlib/execution/coro/queues.hpp>
#include <memory>
#include <thread>
#include <vector>

namespace
{

using hwlib::execution::MpmcQueue;
using hwlib::execution::SpscQueue;
using hwlib::execution::test::Tracked;

template<typename Queue>
void FillsDrainsAndRefusesInOrder()
{
    Queue queue;
    for (int i = 0; i < 4; ++i)
    {
        ASSERT_TRUE(queue.TryPush(int{i}));
    }
    int extra = 99;
    EXPECT_FALSE(queue.TryPush(std::move(extra)));
    for (int i = 0; i < 4; ++i)
    {
        EXPECT_EQ(queue.TryPop(), std::optional<int>{i});
    }
    EXPECT_FALSE(queue.TryPop().has_value());
    // Around the wrap of the indices.
    for (int round = 0; round < 10; ++round)
    {
        ASSERT_TRUE(queue.TryPush(int{round}));
        EXPECT_EQ(queue.TryPop(), std::optional<int>{round});
    }
}

TEST(SpscQueue, FillsDrainsAndRefusesInOrder)
{
    FillsDrainsAndRefusesInOrder<SpscQueue<int, 4>>();
}

TEST(MpmcQueue, FillsDrainsAndRefusesInOrder)
{
    FillsDrainsAndRefusesInOrder<MpmcQueue<int, 4>>();
}

template<typename Queue>
void AFailedPushLeavesTheValue()
{
    Queue queue;
    ASSERT_TRUE(queue.TryPush(std::make_unique<int>(1)));
    auto kept = std::make_unique<int>(2);
    EXPECT_FALSE(queue.TryPush(std::move(kept)));
    ASSERT_NE(kept, nullptr); // NOLINT(bugprone-use-after-move): not moved on failure
    EXPECT_EQ(*kept, 2);
}

TEST(SpscQueue, AFailedPushLeavesTheValue)
{
    AFailedPushLeavesTheValue<SpscQueue<std::unique_ptr<int>, 1>>();
}

TEST(MpmcQueue, AFailedPushLeavesTheValue)
{
    AFailedPushLeavesTheValue<MpmcQueue<std::unique_ptr<int>, 1>>();
}

template<typename Queue>
void DestroysWhatIsStillQueued()
{
    // a139's queues had no destructor: queued objects were never destroyed.
    Tracked::alive = 0;
    {
        Queue queue;
        ASSERT_TRUE(queue.TryPush(Tracked{}));
        ASSERT_TRUE(queue.TryPush(Tracked{}));
        EXPECT_EQ(Tracked::alive, 2);
    }
    EXPECT_EQ(Tracked::alive, 0);
}

TEST(SpscQueue, DestroysWhatIsStillQueued)
{
    DestroysWhatIsStillQueued<SpscQueue<Tracked, 4>>();
}

TEST(MpmcQueue, DestroysWhatIsStillQueued)
{
    DestroysWhatIsStillQueued<MpmcQueue<Tracked, 4>>();
}

TEST(SpscQueue, OneProducerOneConsumerAcrossThreads)
{
    constexpr int COUNT = 100000;
    SpscQueue<int, 64> queue;
    std::thread producer{[&] {
        for (int i = 0; i < COUNT; ++i)
        {
            queue.Push(int{i});
        }
    }};
    long long sum = 0;
    bool inOrder  = true;
    for (int i = 0; i < COUNT; ++i)
    {
        const int value  = queue.Pop();
        inOrder          = inOrder && value == i;
        sum             += value;
    }
    producer.join();
    EXPECT_TRUE(inOrder);
    EXPECT_EQ(sum, static_cast<long long>(COUNT) * (COUNT - 1) / 2);
}

TEST(MpmcQueue, SeveralProducersAndConsumersLoseNothing)
{
    constexpr int PER_PRODUCER = 20000;
    constexpr int PRODUCERS    = 3;
    MpmcQueue<int, 64> queue;
    std::atomic<long long> sum{0};
    std::atomic<int> popped{0};
    std::vector<std::thread> threads;
    for (int p = 0; p < PRODUCERS; ++p)
    {
        threads.emplace_back([&] {
            for (int i = 1; i <= PER_PRODUCER; ++i)
            {
                while (!queue.TryPush(int{i}))
                {
                    std::this_thread::yield();
                }
            }
        });
    }
    for (int c = 0; c < 2; ++c)
    {
        threads.emplace_back([&] {
            while (popped.load() < PER_PRODUCER * PRODUCERS)
            {
                if (const auto value = queue.TryPop(); value.has_value())
                {
                    sum += *value;
                    ++popped;
                }
                else
                {
                    std::this_thread::yield();
                }
            }
        });
    }
    for (auto& thread : threads)
    {
        thread.join();
    }
    EXPECT_EQ(sum.load(), static_cast<long long>(PRODUCERS) * PER_PRODUCER * (PER_PRODUCER + 1) / 2);
}

} // namespace
