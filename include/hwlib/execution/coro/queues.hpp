#pragma once

#include <array>
#include <atomic>
#include <bit>
#include <concepts>
#include <cstddef>
#include <memory>
#include <new>
#include <optional>
#include <type_traits>
#include <utility>

namespace hwlib::execution
{

namespace detail
{

/// Fixed storage for N objects of T whose lifetimes the queue manages.
template<typename T, std::size_t N>
class QueueSlots
{
public:
    template<typename V>
    void Construct(std::size_t index, V&& value) noexcept(std::is_nothrow_constructible_v<T, V&&>)
    {
        std::construct_at(Address(index), std::forward<V>(value));
    }

    /// Moves the object out and ends its lifetime.
    [[nodiscard]] T Take(std::size_t index) noexcept(std::is_nothrow_move_constructible_v<T>)
    {
        T* const slot = std::launder(Address(index));
        T value{std::move(*slot)};
        std::destroy_at(slot);
        return value;
    }

private:
    [[nodiscard]] T* Address(std::size_t index) noexcept
    {
        return reinterpret_cast<T*>(m_storage[index & (N - 1U)].bytes.data()); // NOLINT: raw storage
    }

    struct alignas(T) Slot
    {
        std::array<std::byte, sizeof(T)> bytes;
    };

    std::array<Slot, N> m_storage{};
};

/// Holds an atomic_flag as a try-lock for its lifetime.
class TryLockGuard
{
public:
    explicit TryLockGuard(std::atomic_flag& flag) noexcept
        : m_flag{flag}
        , m_owns{!flag.test_and_set(std::memory_order_acquire)}
    {}

    TryLockGuard(const TryLockGuard&)            = delete;
    TryLockGuard& operator=(const TryLockGuard&) = delete;
    TryLockGuard(TryLockGuard&&)                 = delete;
    TryLockGuard& operator=(TryLockGuard&&)      = delete;

    ~TryLockGuard()
    {
        if (m_owns)
        {
            m_flag.clear(std::memory_order_release);
        }
    }

    [[nodiscard]] bool OwnsLock() const noexcept
    {
        return m_owns;
    }

private:
    std::atomic_flag& m_flag;
    bool m_owns;
};

} // namespace detail

/// A bounded queue for one producer thread and one consumer thread, lock-free.
/// N is a power of two. Push() and Pop() block (atomic wait) where the standard
/// library has it; TryPush() and TryPop() never do.
///
/// Push takes an rvalue: a139 also took an lvalue and silently moved from it. A
/// failed TryPush() leaves the value where it was.
template<std::movable T, std::size_t N>
    requires(std::has_single_bit(N))
class SpscQueue
{
public:
    SpscQueue()                            = default;
    SpscQueue(const SpscQueue&)            = delete;
    SpscQueue& operator=(const SpscQueue&) = delete;
    SpscQueue(SpscQueue&&)                 = delete;
    SpscQueue& operator=(SpscQueue&&)      = delete;

    /// Ends the lifetime of what is still queued; a139 leaked it.
    ~SpscQueue()
    {
        while (TryPop().has_value())
        {}
    }

    [[nodiscard]] bool TryPush(T&& value) noexcept(std::is_nothrow_move_constructible_v<T>)
    {
        const std::size_t read  = m_read.load(std::memory_order_acquire);
        const std::size_t write = m_write.load(std::memory_order_relaxed);
        if (read - write == 0U)
        {
            return false;
        }
        m_slots.Construct(write, std::move(value));
        m_write.store(write + 1U, std::memory_order_release);
        Notify(m_write);
        return true;
    }

    [[nodiscard]] std::optional<T> TryPop() noexcept(std::is_nothrow_move_constructible_v<T>)
    {
        const std::size_t write = m_write.load(std::memory_order_acquire);
        const std::size_t read  = m_read.load(std::memory_order_relaxed);
        if (read - write == N)
        {
            return std::nullopt;
        }
        std::optional<T> value{m_slots.Take(read)};
        m_read.store(read + 1U, std::memory_order_release);
        Notify(m_read);
        return value;
    }

#if defined(__cpp_lib_atomic_wait)
    void Push(T&& value) noexcept(std::is_nothrow_move_constructible_v<T>)
    {
        const std::size_t write = m_write.load(std::memory_order_relaxed);
        std::size_t read        = m_read.load(std::memory_order_acquire);
        while (read - write == 0U)
        {
            m_read.wait(read, std::memory_order_acquire);
            read = m_read.load(std::memory_order_acquire);
        }
        m_slots.Construct(write, std::move(value));
        m_write.store(write + 1U, std::memory_order_release);
        m_write.notify_one();
    }

    [[nodiscard]] T Pop() noexcept(std::is_nothrow_move_constructible_v<T>)
    {
        const std::size_t read = m_read.load(std::memory_order_relaxed);
        std::size_t write      = m_write.load(std::memory_order_acquire);
        while (read - write == N)
        {
            m_write.wait(write, std::memory_order_acquire);
            write = m_write.load(std::memory_order_acquire);
        }
        T value = m_slots.Take(read);
        m_read.store(read + 1U, std::memory_order_release);
        m_read.notify_one();
        return value;
    }
#endif

private:
    static void Notify([[maybe_unused]] std::atomic_size_t& index) noexcept
    {
#if defined(__cpp_lib_atomic_wait)
        index.notify_one();
#endif
    }

    // Free slots are m_read - m_write: N when empty, 0 when full.
    std::atomic_size_t m_read{N};
    std::atomic_size_t m_write{0U};
    detail::QueueSlots<T, N> m_slots;
};

/// A bounded queue for any number of producers and consumers, guarded by a
/// try-lock: TryPush() and TryPop() never wait, so both are safe from an interrupt
/// — and both fail, as if full or empty, while another thread holds the lock.
template<std::movable T, std::size_t N>
    requires(std::is_nothrow_move_constructible_v<T> && std::has_single_bit(N))
class MpmcQueue
{
public:
    MpmcQueue()                            = default;
    MpmcQueue(const MpmcQueue&)            = delete;
    MpmcQueue& operator=(const MpmcQueue&) = delete;
    MpmcQueue(MpmcQueue&&)                 = delete;
    MpmcQueue& operator=(MpmcQueue&&)      = delete;

    /// Ends the lifetime of what is still queued; a139 leaked it.
    ~MpmcQueue()
    {
        while (TryPop().has_value())
        {}
    }

    [[nodiscard]] bool TryPush(T&& value) noexcept
    {
        const detail::TryLockGuard lock{m_lock};
        if (!lock.OwnsLock() || m_read - m_write == 0U)
        {
            return false;
        }
        m_slots.Construct(m_write++, std::move(value));
        return true;
    }

    [[nodiscard]] std::optional<T> TryPop() noexcept
    {
        const detail::TryLockGuard lock{m_lock};
        if (!lock.OwnsLock() || m_read - m_write == N)
        {
            return std::nullopt;
        }
        return std::optional<T>{m_slots.Take(m_read++)};
    }

private:
    std::atomic_flag m_lock;
    std::size_t m_read{N};
    std::size_t m_write{0U};
    detail::QueueSlots<T, N> m_slots;
};

} // namespace hwlib::execution
