#include "http_request.hpp"

std::optional<HttpRequestLine> parseHttpRequestLine(std::string_view raw) {
    if (raw.empty()) {
        return std::nullopt;
    }

    const std::size_t lineEnd = raw.find("\r\n");
    if (lineEnd == std::string_view::npos) {
        return std::nullopt;
    }

    const std::string_view requestLine = raw.substr(0, lineEnd);
    const std::size_t methodEnd = requestLine.find(' ');
    if (methodEnd == std::string_view::npos || methodEnd == 0) {
        return std::nullopt;
    }

    const std::size_t pathStart = methodEnd + 1;
    const std::size_t pathEnd = requestLine.find(' ', pathStart);
    if (pathEnd == std::string_view::npos || pathEnd == pathStart || pathEnd + 1 == requestLine.size()) {
        return std::nullopt;
    }

    return HttpRequestLine{
        std::string(requestLine.substr(0, methodEnd)),
        std::string(requestLine.substr(pathStart, pathEnd - pathStart)),
        std::string(requestLine.substr(pathEnd + 1))
    };
}