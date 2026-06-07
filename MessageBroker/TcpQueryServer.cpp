#include "TcpQueryServer.h"

#include <iostream>
#include <cstring>

TcpQueryServer::TcpQueryServer(unsigned int port, MsgBroker* broker)
    : port(port), broker(broker)
{
    WSADATA wsaData;

    if (WSAStartup(MAKEWORD(2, 2), &wsaData) != 0)
    {
        throw std::runtime_error("TCP WSAStartup failed");
    }

    listen_scoket = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);

    if (listen_scoket == INVALID_SOCKET)
    {
        WSACleanup();
        throw std::runtime_error("TCP socket creation failed");
    }
}

TcpQueryServer::~TcpQueryServer()
{
    stop();

    if (listen_scoket != INVALID_SOCKET)
    {
        closesocket(listen_scoket);
        listen_scoket = INVALID_SOCKET;
    }

    WSACleanup();
}

bool TcpQueryServer::binding()
{
    sockaddr_in serverAddress;
    ZeroMemory(&serverAddress, sizeof(serverAddress));

    serverAddress.sin_family = AF_INET;
    serverAddress.sin_addr.s_addr = INADDR_ANY;
    serverAddress.sin_port = htons(static_cast<u_short>(port));

    if (bind( listen_scoket, reinterpret_cast<sockaddr*>(&serverAddress), sizeof(serverAddress)) == SOCKET_ERROR)
    {
        std::cerr << "TCP bind failed: " << WSAGetLastError() << std::endl;
        return false;
    }

    if (listen(listen_scoket, BACKLOG_COUNT) == SOCKET_ERROR)
    {
        std::cerr << "TCP listen failed: " << WSAGetLastError() << std::endl;
        return false;
    }

    std::cout << "TCP Query Server Start. port: " << port << std::endl;

    return true;
}

void TcpQueryServer::start()
{
    running = true;
    accept_thread = std::thread(&TcpQueryServer::acceptLoop, this);
}

void TcpQueryServer::stop()
{
    running = false;

    if (listen_scoket != INVALID_SOCKET)
    {
        closesocket(listen_scoket);
        listen_scoket = INVALID_SOCKET;
    }

    if (accept_thread.joinable())
    {
        accept_thread.join();
    }

    for (auto& thread : client_thread)
    {
        if (thread.joinable())
        {
            thread.join();
        }
    }
}

void TcpQueryServer::acceptLoop()
{
    while (running.load())
    {
        sockaddr_in clientAddress;
        int clientAddressSize = sizeof(clientAddress);

        SOCKET clientSocket = accept(
            listen_scoket,
            reinterpret_cast<sockaddr*>(&clientAddress),
            &clientAddressSize);

        if (clientSocket == INVALID_SOCKET)
        {
            if (running.load())
            {
                std::cerr << "TCP accept failed: " << WSAGetLastError() << std::endl;
            }

            continue;
        }

        client_thread.emplace_back(
            &TcpQueryServer::clientWorker,
            this,
            clientSocket);
    }
}

void TcpQueryServer::clientWorker(SOCKET clientSocket)
{
    while (running.load())
    {
        QueryRequest request;

        if (recvAll( clientSocket, reinterpret_cast<char*>(&request), sizeof(request)) == false ) break;

        uint32_t topic = ntohl(request.topic);
        uint32_t offset = ntohl(request.offset);

        std::vector<unsigned char> outBuffer;

        MsgBroker::code result =
            broker->getMessage(
                static_cast<int>(topic),
                offset,
                outBuffer);

        QueryResponseHeader responseHeader;
        responseHeader.resultCode = htonl(static_cast<uint32_t>(result));
        responseHeader.payloadLength = htonl(static_cast<uint32_t>(outBuffer.size()));

        if (sendAll(
            clientSocket,
            reinterpret_cast<const char*>(&responseHeader),
            sizeof(responseHeader)) == false)
        {
            break;
        }

        if (result == MsgBroker::code_ok && outBuffer.empty() == false)
        {
            if (sendAll(
                clientSocket,
                reinterpret_cast<const char*>(outBuffer.data()),
                static_cast<int>(outBuffer.size())) == false)
            {
                break;
            }
        }
    }

    closesocket(clientSocket);
}

bool TcpQueryServer::recvAll(SOCKET clientSocket, char* buffer, int length)
{
    int receivedSize = 0;

    while (receivedSize < length)
    {
        int result = recv(clientSocket, buffer + receivedSize, length - receivedSize, 0);

        if (result <= 0)
        {
            return false;
        }

        receivedSize += result;
    }

    return true;
}

bool TcpQueryServer::sendAll(SOCKET clientSocket, const char* buffer, int length)
{
    int sentSize = 0;

    while (sentSize < length)
    {
        int result = send(clientSocket, buffer + sentSize, length - sentSize, 0);

        if (result <= 0)
        {
            return false;
        }

        sentSize += result;
    }

    return true;
}