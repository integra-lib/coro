#pragma once

#include <coroutine>
#include <cstddef>
#include <exception>
#include <iterator>
#include <memory>
#include <type_traits>
#include <utility>

namespace hwlib::execution
{

template<typename T>
class AsyncGenerator;

namespace detail
{

struct AsyncGeneratorPromiseBase
{
    /// Hands control back to whoever asked for the next value.
    struct YieldAwaiter
    {
        std::coroutine_handle<> consumer;

        [[nodiscard]] bool await_ready() const noexcept
        {
            return false;
        }

        [[nodiscard]] std::coroutine_handle<> await_suspend(std::coroutine_handle<>) const noexcept
        {
            return consumer;
        }

        void await_resume() const noexcept {}
    };

    [[nodiscard]] std::suspend_always initial_suspend() const noexcept
    {
        return {};
    }

    [[nodiscard]] YieldAwaiter final_suspend() noexcept
    {
        currentValue = nullptr;
        return YieldAwaiter{consumer};
    }

    void unhandled_exception() noexcept
    {
#if defined(__cpp_exceptions)
        exception = std::current_exception();
#else
        std::terminate();
#endif
    }

    void return_void() const noexcept {}

    [[nodiscard]] bool Finished() const noexcept
    {
#if defined(__cpp_exceptions)
        return currentValue == nullptr || static_cast<bool>(exception);
#else
        return currentValue == nullptr;
#endif
    }

    void RethrowIfFailed()
    {
#if defined(__cpp_exceptions)
        if (exception)
        {
            std::rethrow_exception(std::exchange(exception, nullptr));
        }
#endif
    }

    void* currentValue{nullptr};
    std::coroutine_handle<> consumer;
#if defined(__cpp_exceptions)
    std::exception_ptr exception;
#endif
};

template<typename T>
struct AsyncGeneratorPromise final : AsyncGeneratorPromiseBase
{
    using ValueType = std::remove_reference_t<T>;

    [[nodiscard]] AsyncGenerator<T> get_return_object() noexcept;

    /// The value lives in the generator's frame until the consumer asks for the
    /// next one; only its address is kept.
    [[nodiscard]] YieldAwaiter yield_value(ValueType& value) noexcept
    {
        currentValue = std::addressof(value);
        return YieldAwaiter{consumer};
    }

    [[nodiscard]] YieldAwaiter yield_value(ValueType&& value) noexcept
    {
        return yield_value(value);
    }

    [[nodiscard]] ValueType& Value() const noexcept
    {
        return *static_cast<ValueType*>(currentValue);
    }
};

template<typename T>
class AsyncGeneratorIterator final
{
    using Handle = std::coroutine_handle<AsyncGeneratorPromise<T>>;

public:
    using iterator_concept  = std::input_iterator_tag;
    using iterator_category = std::input_iterator_tag;
    using difference_type   = std::ptrdiff_t;
    using value_type        = std::remove_reference_t<T>;
    using reference         = value_type&;
    using pointer           = value_type*;

    explicit AsyncGeneratorIterator(Handle generator) noexcept
        : m_generator{generator}
    {}

    /// `co_await ++it`: resumes the generator for its next value; the iterator
    /// becomes the end one when the generator returns.
    [[nodiscard]] auto operator++() noexcept
    {
        struct Awaiter
        {
            AsyncGeneratorIterator& iterator;

            [[nodiscard]] bool await_ready() const noexcept
            {
                return false;
            }

            [[nodiscard]] std::coroutine_handle<> await_suspend(std::coroutine_handle<> consumer) noexcept
            {
                iterator.m_generator.promise().consumer = consumer;
                return iterator.m_generator;
            }

            AsyncGeneratorIterator& await_resume()
            {
                if (iterator.m_generator.promise().Finished())
                {
                    const Handle generator = std::exchange(iterator.m_generator, nullptr);
                    generator.promise().RethrowIfFailed();
                }
                return iterator;
            }
        };

        return Awaiter{*this};
    }

    [[nodiscard]] reference operator*() const noexcept
    {
        return m_generator.promise().Value();
    }

    [[nodiscard]] pointer operator->() const noexcept
    {
        return std::addressof(m_generator.promise().Value());
    }

    [[nodiscard]] bool operator==(const AsyncGeneratorIterator& other) const noexcept = default;

private:
    Handle m_generator;
};

} // namespace detail

/// A coroutine that co_yields values the consumer awaits one at a time:
///
///     for (auto it = co_await gen.Begin(); it != gen.End();)
///     {
///         use(*it);
///         co_await ++it; // here, not as the increment: GCC 13 rejects that in a template
///     }
///
/// Values are references into the generator's frame, valid until the next `++`.
/// `AsyncGenerator<T&&>` behaves as `AsyncGenerator<T>`; a139's own `T&&`
/// specialisation declared a get_return_object() it never defined and is gone.
template<typename T>
class [[nodiscard]] AsyncGenerator
{
public:
    using promise_type = detail::AsyncGeneratorPromise<T>;
    using Iterator     = detail::AsyncGeneratorIterator<T>;

    explicit AsyncGenerator(std::coroutine_handle<promise_type> handle) noexcept
        : m_handle{handle}
    {}

    AsyncGenerator(const AsyncGenerator&)            = delete;
    AsyncGenerator& operator=(const AsyncGenerator&) = delete;

    AsyncGenerator(AsyncGenerator&& other) noexcept
        : m_handle{std::exchange(other.m_handle, nullptr)}
    {}

    /// a139 overwrote the handle without destroying the frame it held.
    AsyncGenerator& operator=(AsyncGenerator&& other) noexcept
    {
        if (this != &other)
        {
            Destroy();
            m_handle = std::exchange(other.m_handle, nullptr);
        }
        return *this;
    }

    ~AsyncGenerator()
    {
        Destroy();
    }

    /// Runs the generator to its first value.
    [[nodiscard]] auto Begin() const noexcept
    {
        struct Awaiter
        {
            std::coroutine_handle<promise_type> producer;

            [[nodiscard]] bool await_ready() const noexcept
            {
                return !producer;
            }

            [[nodiscard]] std::coroutine_handle<> await_suspend(std::coroutine_handle<> consumer) noexcept
            {
                producer.promise().consumer = consumer;
                return producer;
            }

            [[nodiscard]] Iterator await_resume()
            {
                if (!producer)
                {
                    return Iterator{nullptr};
                }
                if (producer.promise().Finished())
                {
                    producer.promise().RethrowIfFailed();
                    return Iterator{nullptr};
                }
                return Iterator{producer};
            }
        };

        return Awaiter{m_handle};
    }

    [[nodiscard]] Iterator End() const noexcept
    {
        return Iterator{nullptr};
    }

private:
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
AsyncGenerator<T> detail::AsyncGeneratorPromise<T>::get_return_object() noexcept
{
    return AsyncGenerator<T>{std::coroutine_handle<AsyncGeneratorPromise>::from_promise(*this)};
}

} // namespace hwlib::execution
