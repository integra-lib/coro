#include <hwlib/execution/coro.hpp>

namespace hwlib::execution
{

namespace
{
Task<> Nothing()
{
    co_return;
}
} // namespace

Task<int> SecondUnitTask()
{
    co_await Nothing();
    co_return 2;
}

} // namespace hwlib::execution
