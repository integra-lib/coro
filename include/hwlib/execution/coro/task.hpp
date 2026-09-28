#pragma once

#include <coroutine>
#include <exception>
#include <hwlib/execution/coro/detail/result_store.hpp>
#include <utility>

#if defined(__cpp_exceptions)
#include <stdexcept>
#endif

namespace hwlib::execution
{

#if defined(__cpp_exceptions)
/// Thrown by awaiting a Task that holds no coroutine — one moved from. Without
/// exceptions the program ends instead.
class BrokenPromise : public std::logic_error
{
public:
    BrokenPromise()
        : std::logic_error{"hwlib::execution::Task: awaited without a coroutine"}
    {}
};
#endif

template<typename T>
class Task;

namespace detail
{

struct TaskPromiseBase
{
    /// Who awaits this task; resumed by symmetric transfer when it finishes.
    std::coroutine_handle<> continuation{std::noop_coroutine()};

    struct FinalAwaiter
    {
        [[nodiscard]] bool await_ready() const noexcept
        {
            return false;
        }

        template<typename P>
        [[nodiscard]] std::coroutine_handle<> await_suspend(std::coroutine_handle<P> finished) noexcept
        {
            return finished.promise().continuation;
        }

        void await_resume() const noexcept {}
    };

    [[nodiscard]] std::suspend_always initial_suspend() const noexcept
    {
        return {};
    }

    [[nodiscard]] FinalAwaiter final_suspend() const noexcept
    {
        return {};
    }
};

template<typename T>
struct TaskPromise
    : TaskPromiseBase
    , ResultStore<T>
{
    [[nodiscard]] Task<T> get_return_object() noexcept;

    void unhandled_exception() noexcept
    {
        this->SetException();
    }

    template<typename V>
        requires std::convertible_to<V&&, T>
    void return_value(V&& value)
    {
        this->SetValue(std::forward<V>(value));
    }
};

template<typename T>
struct TaskPromise<T&>
    : TaskPromiseBase
    , ResultStore<T&>
{
    [[nodiscard]] Task<T&> get_return_object() noexcept;

    void unhandled_exception() noexcept
    {
        this->SetException();
    }

    void return_value(T& value) noexcept
    {
        this->SetValue(value);
    }
};

template<>
struct TaskPromise<void>
    : TaskPromiseBase
    , ResultStore<void>
{
    [[nodiscard]] inline Task<void> get_return_object() noexcept;

    void unhandled_exception() noexcept
    {
        this->SetException();
    }

    void return_void() const noexcept {}
};

[[noreturn]] inline void BrokenPromiseAwaited()
{
#if defined(__cpp_exceptions)
    throw BrokenPromise{};
#else
    std::terminate();
#endif
}

} // namespace detail

/// A lazily started coroutine: it runs when awaited — or when Resume() is called
/// from ordinary code — and resumes its awaiter when it finishes. Owns the
/// coroutine frame.
template<typename T = void>
class [[nodiscard]] Task
{
public:
    using promise_type = detail::TaskPromise<T>;
    using ValueType    = T;

    explicit Task(std::coroutine_handle<promise_type> handle) noexcept
        : m_handle{handle}
    {}

    Task(const Task&)            = delete;
    Task& operator=(const Task&) = delete;

    Task(Task&& other) noexcept
        : m_handle{std::exchange(other.m_handle, nullptr)}
    {}

    Task& operator=(Task&& other) noexcept
    {
        if (this != &other)
        {
            Destroy();
            m_handle = std::exchange(other.m_handle, nullptr);
        }
        return *this;
    }

    ~Task()
    {
        Destroy();
    }

    /// Finished, or holds no coroutine.
    [[nodiscard]] bool IsReady() const noexcept
    {
        return !m_handle || m_handle.done();
    }

    /// Starts or continues the coroutine from ordinary code. Ends the program on a
    /// task that holds no coroutine or has finished: resuming either is undefined.
    void Resume()
    {
        if (IsReady())
        {
            std::terminate();
        }
        m_handle.resume();
    }

    /// The finished task's value; rethrows what escaped it. BrokenPromise on a
    /// task that holds no coroutine; ends the program on one not finished yet.
    [[nodiscard]] decltype(auto) Result() &
    {
        if (!m_handle)
        {
            detail::BrokenPromiseAwaited();
        }
        return m_handle.promise().Get();
    }

    [[nodiscard]] decltype(auto) Result() &&
    {
        if (!m_handle)
        {
            detail::BrokenPromiseAwaited();
        }
        return std::move(m_handle.promise()).Get();
    }

    /// Awaits completion without taking the value or the exception.
    [[nodiscard]] auto WhenReady() const noexcept
    {
        struct Awaiter : AwaiterBase
        {
            void await_resume() const noexcept {}
        };

        return Awaiter{{m_handle}};
    }

    [[nodiscard]] auto operator co_await() const& noexcept
    {
        struct Awaiter : AwaiterBase
        {
            decltype(auto) await_resume()
            {
                if (!this->task)
                {
                    detail::BrokenPromiseAwaited();
                }
                return this->task.promise().Get();
            }
        };

        return Awaiter{{m_handle}};
    }

    [[nodiscard]] auto operator co_await() const&& noexcept
    {
        struct Awaiter : AwaiterBase
        {
            decltype(auto) await_resume()
            {
                if (!this->task)
                {
                    detail::BrokenPromiseAwaited();
                }
                return std::move(this->task.promise()).Get();
            }
        };

        return Awaiter{{m_handle}};
    }

private:
    struct AwaiterBase
    {
        std::coroutine_handle<promise_type> task;

        [[nodiscard]] bool await_ready() const noexcept
        {
            return !task || task.done();
        }

        [[nodiscard]] std::coroutine_handle<> await_suspend(std::coroutine_handle<> awaiter) noexcept
        {
            task.promise().continuation = awaiter;
            return task;
        }
    };

    void Destroy() noexcept
    {
        if (m_handle)
        {
            m_handle.destroy();
        }
    }

    std::coroutine_handle<promise_type> m_handle;
};

template<typename T>
Task<T> detail::TaskPromise<T>::get_return_object() noexcept
{
    return Task<T>{std::coroutine_handle<TaskPromise>::from_promise(*this)};
}

template<typename T>
Task<T&> detail::TaskPromise<T&>::get_return_object() noexcept
{
    return Task<T&>{std::coroutine_handle<TaskPromise>::from_promise(*this)};
}

// inline: a header defines it, and a139's copy broke the link of a program that
// included this header from two translation units.
inline Task<void> detail::TaskPromise<void>::get_return_object() noexcept
{
    return Task<void>{std::coroutine_handle<TaskPromise>::from_promise(*this)};
}

} // namespace hwlib::execution
