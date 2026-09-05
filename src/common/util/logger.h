#pragma once

#include <algorithm>
#include <cstdlib>
#include <iostream>
#include <ostream>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

namespace Logger {

namespace detail {

inline bool colorEnabled() {
    const char* noColor = std::getenv("NO_COLOR");
    return noColor == nullptr || noColor[0] == '\0';
}

inline std::string color(const std::string& text, const char* code) {
    if (!colorEnabled()) return text;
    return std::string(code) + text + "\033[0m";
}

inline std::string repeat(char ch, size_t count) {
    return std::string(count, ch);
}

inline std::string padRight(const std::string& text, size_t width) {
    if (text.size() >= width) return text;
    return text + repeat(' ', width - text.size());
}

inline std::string center(const std::string& text, size_t width) {
    if (text.size() >= width) return text;
    size_t left = (width - text.size()) / 2;
    size_t right = width - text.size() - left;
    return repeat(' ', left) + text + repeat(' ', right);
}

inline size_t maxValueWidth(const std::vector<std::pair<std::string, std::string>>& rows) {
    size_t width = 0;
    for (const auto& row : rows) {
        width = std::max(width, row.first.size() + row.second.size());
    }
    return width;
}

}  // namespace detail

inline std::string toString(const std::string& value) {
    return value;
}

inline const char* dim() {
    return "\033[2m";
}

inline const char* blue() {
    return "\033[90m";
}

inline const char* green() {
    return "\033[32m";
}

inline const char* yellow() {
    return "\033[33m";
}

inline const char* red() {
    return "\033[31m";
}

inline const char* cyan() {
    return "\033[90m";
}

inline void title(const std::string& text) {
    constexpr size_t width = 56;
    std::cout << "\n"
              << detail::color("+" + detail::repeat('-', width - 2) + "+", cyan()) << "\n"
              << detail::color("|" + detail::center(text, width - 2) + "|", cyan()) << "\n"
              << detail::color("+" + detail::repeat('-', width - 2) + "+", cyan()) << "\n";
}

inline void info(const std::string& text) {
    std::cout << detail::color("[INFO] ", blue()) << text << "\n";
}

inline void success(const std::string& text) {
    std::cout << detail::color("[OK]   ", green()) << text << "\n";
}

inline void warn(const std::string& text) {
    std::cout << detail::color("[WARN] ", yellow()) << text << "\n";
}

inline void error(const std::string& text) {
    std::cerr << detail::color("[ERR]  ", red()) << text << "\n";
}

inline void activity(const std::string& text) {
    std::cout << detail::color("[LOG]  ", dim()) << text << std::endl;
}

inline void panel(const std::string& heading,
                  const std::vector<std::pair<std::string, std::string>>& rows,
                  std::ostream& out = std::cout) {
    constexpr size_t minWidth = 56;
    size_t contentWidth = std::max(minWidth - 4, heading.size() + 2);
    contentWidth = std::max(contentWidth, detail::maxValueWidth(rows) + 5);

    out << "\n"
        << detail::color("+" + detail::repeat('-', contentWidth + 2) + "+", cyan()) << "\n"
        << detail::color("| " + detail::center(heading, contentWidth) + " |", cyan()) << "\n"
        << detail::color("+" + detail::repeat('-', contentWidth + 2) + "+", cyan()) << "\n";

    for (const auto& row : rows) {
        out << "  " << detail::color(detail::padRight(row.first, 11), blue()) << " "
            << row.second << "\n";
    }

    out << detail::color("+" + detail::repeat('-', contentWidth + 2) + "+", cyan()) << "\n";
}

inline void usage(const std::string& command, const std::vector<std::string>& lines) {
    std::cerr << "\n" << detail::color("Usage", yellow()) << "\n";
    std::cerr << "  " << command << "\n";
    for (const auto& line : lines) {
        std::cerr << "  " << line << "\n";
    }
}

}
