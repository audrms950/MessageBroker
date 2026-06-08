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

        if (recvAll(clientSocket, reinterpret_cast<char*>(&request), sizeof(request)) == false)
        {
            break;
        }

        uint32_t topic = ntohl(request.topic);
        uint32_t offset = ntohl(request.offset);
		uint32_t count = ntohl(request.count);


        
        bool disconnected = false;
        uint32_t endOffset = offset + count;

        for (; offset < endOffset; ++offset)
        {

            const char* outBufPtr = nullptr;
            const BrokerMsgBlock::MsgIndex* outBufIdx = nullptr;

            int result = broker->getMessage(
                static_cast<int>(topic),
                offset,
                outBufPtr,
                outBufIdx);

            uint32_t payloadLength = 0;

            if (result == MsgBroker::code_ok && outBufPtr != nullptr && outBufIdx != nullptr)
            {
                payloadLength = static_cast<uint32_t>(outBufIdx->length);
            }

            QueryResponseHeader responseHeader;
            responseHeader.resultCode = htonl(static_cast<uint32_t>(result));
            responseHeader.payloadLength = htonl(payloadLength);
#if(0) /*
            묶어 보내는 방식은 구현은 쉽지만,
            수신 측에서 전체 응답 크기를 사전에 알 수 없어 recvAll 기반 안정적인 수신이 어려움.
            따라서 현재 구조에서는 header 수신 후 payloadLength 기준으로 payload를 분리 수신하는 방식을 유지
        */
            std::vector<char> sendBuffer;
            sendBuffer.resize(sizeof(QueryResponseHeader) + payloadLength);

            std::memcpy(sendBuffer.data(), &responseHeader, sizeof(responseHeader));
            std::memcpy(
                sendBuffer.data() + sizeof(responseHeader),
                outBufPtr + outBufIdx->start_offset,
                payloadLength);

            if (sendAll(clientSocket, sendBuffer.data(), static_cast<int>(sendBuffer.size())) == false)
            {
                disconnected = true;
                break;
            }
#endif 
            if (sendAll(
                clientSocket,
                reinterpret_cast<const char*>(&responseHeader),
                sizeof(responseHeader)) == false)
            {
                disconnected = true;
                break;
            }

            if (result == MsgBroker::code_ok && payloadLength != 0)
            {
                if (sendAll(
                    clientSocket,
                    outBufPtr + outBufIdx->start_offset,
                    static_cast<int>(payloadLength)) == false)
                {
                    disconnected = true;
                    break;
                }
            }
        }

        if (disconnected == true)
        {
            break;
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