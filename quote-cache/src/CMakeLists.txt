cmake_minimum_required(VERSION 3.11.1)
project(quote_cache)

set(CMAKE_CXX_STANDARD 14)
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY /out)

set(Boost_USE_STATIC_LIBS OFF)
set(Boost_USE_MULTITHREADED ON)
set(Boost_USE_STATIC_RUNTIME OFF)
find_package(Boost 1.66.0 COMPONENTS system)

if(Boost_FOUND)
    include_directories(${Boost_INCLUDE_DIRS})
    add_executable(quote-cache main.cpp)
    target_link_libraries(quote-cache ${Boost_LIBRARIES})
endif()

