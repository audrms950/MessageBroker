#pragma once
#pragma comment(lib, "Ws2_32.lib")

#include <iostream>
#include <winsock2.h>
#include <ws2tcpip.h>
#include <stdexcept>
#include <string>
#include <vector>
#include <cstdint>
#include <future>
#include "MsgBroker.h"

class ProducerMessage
{
private:
    SOCKET socketHandle = INVALID_SOCKET;

private:
    const int port;
    static constexpr int BUFFER_COUNT = 2;
    static constexpr int BLOCK_BUFFER_SIZE = 1024 * 1024 * 16;

public:
    explicit ProducerMessage(int port) : port(port)
    {
        WSADATA wsaData;

        if (WSAStartup(MAKEWORD(2, 2), &wsaData) != 0)
        {
            throw std::runtime_error("Socket initialization failed");
        }

        socketHandle = socket(AF_INET, SOCK_DGRAM, IPPROTO_UDP);

        if (socketHandle == INVALID_SOCKET)
        {
            WSACleanup();
            throw std::runtime_error("Socket creation failed");
        }
    }

    ~ProducerMessage()
    {
        if (socketHandle != INVALID_SOCKET)
        {
            closesocket(socketHandle);
            socketHandle = INVALID_SOCKET;
        }

        WSACleanup();
    }

    ProducerMessage& binding()
    {
        sockaddr_in serverAddress;
        ZeroMemory(&serverAddress, sizeof(serverAddress));

        serverAddress.sin_family = AF_INET;
        serverAddress.sin_addr.s_addr = INADDR_ANY;
        serverAddress.sin_port = htons(port);

        if (bind(socketHandle,
            reinterpret_cast<sockaddr*>(&serverAddress),
            sizeof(serverAddress)) == SOCKET_ERROR)
        {
            closesocket(socketHandle);
            socketHandle = INVALID_SOCKET;
            WSACleanup();

            throw std::runtime_error("Socket binding failed");
        }

        std::cout << "UDP Server Start" << std::endl;

        return *this;
    }

    struct MessageHeader
    {
        uint16_t length; /* 메시지 길이 */
        uint16_t mark;   /* 메시지 구분자 */
        uint16_t topic;  /* 메시지 토픽 */
        
    };


    bool pushBroker(MsgBroker* broker, const std::vector<char>& data, unsigned int usedSize)
    {
        unsigned int offset = 0;

        while (offset + sizeof(MessageHeader) <= usedSize)
        {
            MessageHeader header;

            std::memcpy(
                &header,
                data.data() + offset,
                sizeof(header));

            if (header.length < sizeof(header.mark) + sizeof(header.topic))
            {
                std::cerr << "Invalid message length in header" << std::endl;
                return false;
            }

            unsigned int messageTotalSize = sizeof(uint16_t) + header.length;

            if (offset + messageTotalSize > usedSize)
            {
                std::cerr << "Invalid message boundary" << std::endl;
                return false;
            }

            unsigned int payloadOffset = offset + sizeof(MessageHeader);
            unsigned int payloadLength =
                header.length - sizeof(header.mark) - sizeof(header.topic);

            broker->pushMessage(
                header.topic,
                reinterpret_cast<const unsigned char*>(data.data() + payloadOffset),
                payloadLength);

            offset += messageTotalSize;
        }

        return true;
    }

    void recvBuf(MsgBroker* broker, int topic)
    {
        sockaddr_in clientAddress;
        int clientAddressSize = sizeof(clientAddress);

        int recvedCnt = 0;
        unsigned int bufferIndex = 0;
        unsigned int curOffset = sizeof(uint16_t);

        const unsigned int BOUND_MAX_MSG_SIZE = 1024 * 1024;
        const int TIMEOUT_MS = 50;

        setsockopt(
            socketHandle,
            SOL_SOCKET,
            SO_RCVTIMEO,
            reinterpret_cast<const char*>(&TIMEOUT_MS),
            sizeof(TIMEOUT_MS));

        std::vector<std::vector<char>> blockBuffer(
            BUFFER_COUNT,
            std::vector<char>(BLOCK_BUFFER_SIZE));

        auto flushBuffer = [&]()
            {
                if (curOffset <= sizeof(uint16_t))
                {
                    return;
                }

                broker->pushBatch(topic, blockBuffer[bufferIndex], curOffset - 2);

                bufferIndex = (bufferIndex + 1) % BUFFER_COUNT;
                curOffset = sizeof(uint16_t);
            };

        while (socketHandle != INVALID_SOCKET)
        {
            unsigned int writableSize = BLOCK_BUFFER_SIZE - curOffset;

            if (writableSize < BOUND_MAX_MSG_SIZE + sizeof(uint16_t))
            {
                flushBuffer();
                writableSize = BLOCK_BUFFER_SIZE - curOffset;
            }

            clientAddressSize = sizeof(clientAddress);

            int bytesReceived = recvfrom(
                socketHandle,
                blockBuffer[bufferIndex].data() + curOffset,
                writableSize,
                0,
                reinterpret_cast<sockaddr*>(&clientAddress),
                &clientAddressSize);

            if (bytesReceived == SOCKET_ERROR)
            {
                int errorCode = WSAGetLastError();

                if (errorCode == WSAETIMEDOUT)
                {
                    flushBuffer();
                    continue;
                }

                std::cerr << "recvfrom failed: "
                    << errorCode
                    << std::endl;

                continue;
            }

            if (bytesReceived <= 0)
            {
                continue;
            }

            uint16_t messageLength = static_cast<uint16_t>(bytesReceived);

            std::memcpy(
                blockBuffer[bufferIndex].data() + curOffset - sizeof(uint16_t),
                &messageLength,
                sizeof(messageLength));

            curOffset += bytesReceived + sizeof(uint16_t);
            ++recvedCnt;

            if (recvedCnt % 1000 == 0)
            {
                flushBuffer();
            }
        }

        flushBuffer();
    }
};