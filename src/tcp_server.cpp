#include "http_request.hpp"
#include "tcp_server.hpp"

#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <string>
#include <unistd.h>
#include <string_view>

void runTcpServer() {
    int sockfd = socket(AF_INET, SOCK_STREAM, 0);
    if (sockfd < 0) {
        std::cerr << "Failed to create socket" << std::endl;
        exit(EXIT_FAILURE);
    }
    struct sockaddr_in sockaddr{};
    sockaddr.sin_family = AF_INET;
    sockaddr.sin_port = htons(8080);
    sockaddr.sin_addr.s_addr = INADDR_ANY;

    if (bind(sockfd, reinterpret_cast<struct sockaddr*>(&sockaddr), sizeof(sockaddr)) < 0) {
        std::cerr << "Failed to bind socket to port 8080" << std::endl;
        exit(EXIT_FAILURE);
    }

    if (listen(sockfd, 15) < 0) {
        std::cerr << "Failed to listen on socket" << std::endl;
        exit(EXIT_FAILURE);
    }
    
    std::cout << "Server is listening on port 8080" << std::endl;
    while (true) {
        struct sockaddr_in clientAddr{};
        socklen_t addrlen = sizeof(clientAddr);
        int connection = accept(sockfd, reinterpret_cast<struct sockaddr*>(&clientAddr), &addrlen);
        if (connection < 0) {
            std::cerr << "Failed to accept connection" << std::endl;
            continue;
        }
        char address[INET_ADDRSTRLEN];
        if(inet_ntop(AF_INET, &clientAddr.sin_addr, address, sizeof(address)) == nullptr) {
            std::cerr << "Failed to convert client IP address" << std::endl;
        } else {
            std::cout << "Client IP Address: " << address << std::endl;
        }

        std::cout << "Client Port: " << ntohs(clientAddr.sin_port) << std::endl;
        char buffer[1024];
        ssize_t bytesReceived;
        uint64_t totalBytes = 0;
        std::string received;
        bool headerComplete = false;
        while ((bytesReceived = read(connection, buffer, sizeof(buffer))) > 0) {
            received.append(buffer, static_cast<std::size_t>(bytesReceived));
            std::cout.write(buffer, bytesReceived);
            std::cout.flush();
            totalBytes += static_cast<uint64_t>(bytesReceived);
            if (std::string_view(received).find("\r\n\r\n") != std::string_view::npos) {
                headerComplete = true;
                break;
            }
        }
        if (headerComplete) {
            const auto requestLine = parseHttpRequestLine(received);
            if (requestLine) {
                std::cout << "Request: " << requestLine->method << ' '
                          << requestLine->path << ' ' << requestLine->version << std::endl;
            }
            const std::string response = requestLine
                ? "HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\nContent-Length: 13\r\n\r\nHello, client!"
                : "HTTP/1.1 400 Bad Request\r\nContent-Type: text/plain\r\nContent-Length: 12\r\n\r\nBad Request\n";
            write(connection, response.c_str(), response.size());
        } 
        if(!headerComplete) {
            if(bytesReceived == 0) {
                std::cout << "Client has disconnected" << std::endl;
            } else if (bytesReceived < 0) {
                std::cerr << "Failed to read from client" << std::endl;
            }
        }
        std::cout << "Total bytes received: " << totalBytes << std::endl;
        
        close(connection);
    }
}