/**
 * @file exception.hpp
 * @author Sinter Wong (sintercver@gmail.com)
 * @brief
 * @version 0.1
 * @date 2025-08-18
 *
 * @copyright Copyright (c) 2025
 *
 */
#ifndef AI_PIPE_UTILS_EXCEPTION_HPP
#define AI_PIPE_UTILS_EXCEPTION_HPP

#include <stdexcept>
#include <type_traits>
#include <variant>

namespace ai_pipe::exception {

template <typename T, typename... Types> struct is_one_of;

template <typename T, typename First, typename... Rest>
struct is_one_of<T, First, Rest...> {
  static constexpr bool value =
      std::is_same_v<T, First> || is_one_of<T, Rest...>::value;
};

template <typename T> struct is_one_of<T> {
  static constexpr bool value = false;
};

template <typename T, typename... Ts>
const T &get_or_throw(const std::variant<Ts...> &v) {
  if constexpr (is_one_of<T, Ts...>::value) {
    if (const auto ptr = std::get_if<T>(&v)) {
      return *ptr;
    }
    throw std::runtime_error(
        "Variant does not currently hold the requested type: " +
        std::string(typeid(T).name()));
  } else {
    throw std::runtime_error("Requested type " + std::string(typeid(T).name()) +
                             " is not a valid alternative for this variant");
  }
}

class InvalidValueException : public std::runtime_error {
public:
  explicit InvalidValueException(const std::string &message)
      : std::runtime_error("Invalid value: " + message) {}
};

class OutOfRangeException : public std::out_of_range {
public:
  explicit OutOfRangeException(const std::string &message)
      : std::out_of_range("Out of range: " + message) {}
};

class NullPointerException : public std::logic_error {
public:
  explicit NullPointerException(const std::string &message)
      : std::logic_error("Null pointer: " + message) {}
};

class FileOperationException : public std::runtime_error {
public:
  explicit FileOperationException(const std::string &message)
      : std::runtime_error("File operation error: " + message) {}
};

class NetworkException : public std::runtime_error {
public:
  explicit NetworkException(const std::string &message)
      : std::runtime_error("Network error: " + message) {}
};

class ExecutionException : public std::runtime_error {
public:
  explicit ExecutionException(const std::string &message)
      : std::runtime_error("Execution error: " + message) {}
};
} // namespace ai_pipe::exception

#endif
