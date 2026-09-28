#pragma once

#include <optional>
#include <string>
#include <string_view>

struct HttpRequestLine {
    std::string method;
    std::string path;
    std::string version;
};

std::optional<HttpRequestLine> parseHttpRequestLine(std::string_view raw);