#include "test_support.hpp"

#include <thread>

namespace hwlib::execution::test
{

// In its own translation unit on purpose: see the declaration.
std::thread::id CurrentThreadId() noexcept
{
    return std::this_thread::get_id();
}

} // namespace hwlib::execution::test
