#pragma once

#include <concepts>
#include <coroutine>
#include <type_traits>
#include <utility>

namespace hwlib::execution
{

namespace detail
{

template<typename T>
struct IsCoroutineHandle : std::false_type
{};

template<typename P>
struct IsCoroutineHandle<std::coroutine_handle<P>> : std::true_type
{};

template<typename T>
concept AwaitSuspendResult = std::is_void_v<T> || std::is_same_v<T, bool> || IsCoroutineHandle<T>::value;

/// Has the three awaiter members itself.
template<typename T>
concept Awaiter = requires(T value) {
    {
        value.await_ready()
    } -> std::same_as<bool>;
    {
        value.await_suspend(std::coroutine_handle<>{})
    } -> AwaitSuspendResult;
    value.await_resume();
};

template<typename T>
concept HasMemberCoAwait = requires(T value) { std::forward<T>(value).operator co_await(); };

template<typename T>
concept HasFreeCoAwait = requires(T value) { operator co_await(std::forward<T>(value)); };

/// The awaiter `co_await value` uses, by the language's own order: a member
/// operator co_await, then a free one, then the value itself. Only its type is used.
template<typename T>
decltype(auto) GetAwaiter(T&& value)
{
    if constexpr (HasMemberCoAwait<T>)
    {
        return std::forward<T>(value).operator co_await();
    }
    else if constexpr (HasFreeCoAwait<T>)
    {
        return operator co_await(std::forward<T>(value));
    }
    else
    {
        return std::forward<T>(value);
    }
}

} // namespace detail

/// Something `co_await` accepts.
template<typename T>
concept Awaitable = detail::Awaiter<T> || detail::HasMemberCoAwait<T> || detail::HasFreeCoAwait<T>;

template<Awaitable T>
struct AwaitableTraits
{
    using AwaiterType = decltype(detail::GetAwaiter(std::declval<T>()));
    /// What `co_await` on a T evaluates to.
    using AwaitResult = decltype(std::declval<AwaiterType>().await_resume());
};

/// The handle of the calling coroutine, without suspending it: `auto h = co_await
/// ThisCoroutine();`.
template<typename PromiseType = void>
[[nodiscard]] constexpr auto ThisCoroutine() noexcept
{
    struct GetHandle
    {
        std::coroutine_handle<PromiseType> handle;

        [[nodiscard]] constexpr bool await_ready() const noexcept
        {
            return false;
        }

        [[nodiscard]] constexpr bool await_suspend(std::coroutine_handle<PromiseType> caller) noexcept
        {
            handle = caller;
            return false;
        }

        [[nodiscard]] constexpr std::coroutine_handle<PromiseType> await_resume() const noexcept
        {
            return handle;
        }
    };

    return GetHandle{};
}

} // namespace hwlib::execution
