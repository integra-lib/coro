#pragma once

#include <exception>
#include <memory>
#include <optional>
#include <type_traits>
#include <utility>

namespace hwlib::execution::detail
{

/// Holds what a coroutine produced: its value, or — when exceptions are enabled —
/// the exception that escaped it. Without exceptions nothing can escape; a
/// coroutine that would have thrown ends the program in unhandled_exception().
///
/// Reading a result that was never produced ends the program too: it is a caller
/// error, reading a task before it finished.
template<typename T>
class ResultStore
{
public:
    template<typename V>
    void SetValue(V&& value)
    {
        m_value.emplace(std::forward<V>(value));
    }

    void SetException() noexcept
    {
#if defined(__cpp_exceptions)
        m_exception = std::current_exception();
#else
        std::terminate();
#endif
    }

    [[nodiscard]] T& Get() &
    {
        RethrowOrCheck();
        return *m_value;
    }

    [[nodiscard]] T&& Get() &&
    {
        RethrowOrCheck();
        return std::move(*m_value);
    }

private:
    void RethrowOrCheck()
    {
#if defined(__cpp_exceptions)
        if (m_exception)
        {
            std::rethrow_exception(m_exception);
        }
#endif
        if (!m_value.has_value())
        {
            std::terminate();
        }
    }

    std::optional<T> m_value;
#if defined(__cpp_exceptions)
    std::exception_ptr m_exception;
#endif
};

/// A reference result is kept as the address of the referent: taking it in never
/// moves or copies the object.
template<typename T>
class ResultStore<T&>
{
public:
    void SetValue(T& value) noexcept
    {
        m_value = std::addressof(value);
    }

    void SetException() noexcept
    {
#if defined(__cpp_exceptions)
        m_exception = std::current_exception();
#else
        std::terminate();
#endif
    }

    [[nodiscard]] T& Get() const
    {
#if defined(__cpp_exceptions)
        if (m_exception)
        {
            std::rethrow_exception(m_exception);
        }
#endif
        if (m_value == nullptr)
        {
            std::terminate();
        }
        return *m_value;
    }

private:
    T* m_value{nullptr};
#if defined(__cpp_exceptions)
    std::exception_ptr m_exception;
#endif
};

template<>
class ResultStore<void>
{
public:
    void SetException() noexcept
    {
#if defined(__cpp_exceptions)
        m_exception = std::current_exception();
#else
        std::terminate();
#endif
    }

    void Get() const
    {
#if defined(__cpp_exceptions)
        if (m_exception)
        {
            std::rethrow_exception(m_exception);
        }
#endif
    }

private:
#if defined(__cpp_exceptions)
    std::exception_ptr m_exception;
#endif
};

/// What a coroutine stores for an await result: an rvalue reference becomes the
/// value it refers to, an lvalue reference stays a reference.
template<typename T>
using StoredResult = std::conditional_t<std::is_rvalue_reference_v<T>, std::remove_reference_t<T>, T>;

/// Runs a callable when the scope ends.
template<typename F>
class ScopeExit
{
public:
    explicit ScopeExit(F callable) noexcept
        : m_callable{std::move(callable)}
    {}

    ScopeExit(const ScopeExit&)            = delete;
    ScopeExit& operator=(const ScopeExit&) = delete;
    ScopeExit(ScopeExit&&)                 = delete;
    ScopeExit& operator=(ScopeExit&&)      = delete;

    ~ScopeExit()
    {
        m_callable();
    }

private:
    F m_callable;
};

} // namespace hwlib::execution::detail
