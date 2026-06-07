#include "MsgBroker.h"
#include "ProducerReceiver.h"
#include "TcpQueryServer.h"

#include <winsock2.h>
#include <ws2tcpip.h>

#include <iostream>
#include <vector>
#include <thread>
#include <chrono>
#include <cstring>

#pragma comment(lib, "Ws2_32.lib")

struct TcpQueryRequest
{
    uint32_t topic;
    uint32_t offset;
};

struct TcpQueryResponseHeader
{
    uint32_t resultCode;
    uint32_t payloadLength;
};

bool recvAll(SOCKET socketHandle, char* buffer, int length)
{
    int receivedSize = 0;

    while (receivedSize < length)
    {
        int result = recv(
            socketHandle,
            buffer + receivedSize,
            length - receivedSize,
            0);

        if (result <= 0)
        {
            return false;
        }

        receivedSize += result;
    }

    return true;
}

bool sendAll(SOCKET socketHandle, const char* buffer, int length)
{
    int sentSize = 0;

    while (sentSize < length)
    {
        int result = send(
            socketHandle,
            buffer + sentSize,
            length - sentSize,
            0);

        if (result <= 0)
        {
            return false;
        }

        sentSize += result;
    }

    return true;
}

struct SendHeader
{
	uint16_t mark;
    uint16_t topic;
};

int main()
{
    constexpr int UDP_PORT = 9000;
    constexpr int TCP_PORT = 9100;
    constexpr int TOPIC = 1;
    constexpr int MESSAGE_COUNT = 1000000;
    constexpr int PAYLOAD_SIZE = 1024;
    constexpr auto TEST_TIMEOUT = std::chrono::seconds(10);

    MsgBroker broker;

    ProducerReceiver receiver(TOPIC, UDP_PORT, &broker);
    receiver.binding();
    receiver.start();

    TcpQueryServer queryServer(TCP_PORT, &broker);

    if (queryServer.binding() == false)
    {
        std::cerr << "TCP Query Server binding failed" << std::endl;
        return -1;
    }

    queryServer.start();

    std::this_thread::sleep_for(std::chrono::milliseconds(500));

    WSADATA wsaData;

    if (WSAStartup(MAKEWORD(2, 2), &wsaData) != 0)
    {
        std::cerr << "WSAStartup failed" << std::endl;
        return -1;
    }

    SOCKET udpClientSocket = socket(AF_INET, SOCK_DGRAM, IPPROTO_UDP);

    if (udpClientSocket == INVALID_SOCKET)
    {
        std::cerr << "UDP client socket failed" << std::endl;
        WSACleanup();
        return -1;
    }

    sockaddr_in udpServerAddress;
    ZeroMemory(&udpServerAddress, sizeof(udpServerAddress));

    udpServerAddress.sin_family = AF_INET;
    udpServerAddress.sin_port = htons(UDP_PORT);
    inet_pton(AF_INET, "127.0.0.1", &udpServerAddress.sin_addr);

    std::vector<char> packet(sizeof(SendHeader) + PAYLOAD_SIZE);

    auto totalStartTime = std::chrono::high_resolution_clock::now();
    auto sendStartTime = std::chrono::high_resolution_clock::now();

    int sendSuccessCount = 0;
    int sendFailCount = 0;

    for (int i = 0; i < MESSAGE_COUNT; ++i)
    {
        SendHeader header;

        header.mark = htons(static_cast<uint16_t>(i & 0xFFFF));
        header.topic = htons(static_cast<uint16_t>(TOPIC));

        std::memcpy(packet.data(), &header, sizeof(header));

        std::memset(
            packet.data() + sizeof(header),
            'A' + (i % 26),
            PAYLOAD_SIZE);

        int sentBytes =
            sendto(
                udpClientSocket,
                packet.data(),
                static_cast<int>(packet.size()),
                0,
                reinterpret_cast<sockaddr*>(&udpServerAddress),
                sizeof(udpServerAddress));

        if (sentBytes == SOCKET_ERROR)
        {
            ++sendFailCount;
        }
        else
        {
            ++sendSuccessCount;
        }
    }

    auto sendEndTime = std::chrono::high_resolution_clock::now();

    double sendElapsedSec =
        std::chrono::duration<double>(sendEndTime - sendStartTime).count();

    closesocket(udpClientSocket);

    std::cout << "==============================" << std::endl;
    std::cout << "UDP Send Complete" << std::endl;
    std::cout << "Send Success: " << sendSuccessCount << std::endl;
    std::cout << "Send Fail: " << sendFailCount << std::endl;
    std::cout << "Send Time: " << sendElapsedSec << " sec" << std::endl;
    std::cout << "Send Rate: " << sendSuccessCount / sendElapsedSec << " msg/s" << std::endl;

    auto storeWaitStartTime = std::chrono::high_resolution_clock::now();
    auto storeTimeoutStartTime = std::chrono::steady_clock::now();

    while (broker.getStoredMessageCount() < MESSAGE_COUNT &&
        std::chrono::steady_clock::now() - storeTimeoutStartTime < TEST_TIMEOUT)
    {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }

    auto storeWaitEndTime = std::chrono::high_resolution_clock::now();

    double storeWaitElapsedSec =
        std::chrono::duration<double>(storeWaitEndTime - storeWaitStartTime).count();

    unsigned int storedCount = broker.getStoredMessageCount();

    std::cout << "==============================" << std::endl;

    if (storedCount == MESSAGE_COUNT)
    {
        std::cout << "All messages stored successfully." << std::endl;
    }
    else
    {
        std::cout << "Timeout reached." << std::endl;
    }

    std::cout << "Expected : " << MESSAGE_COUNT << std::endl;
    std::cout << "Stored   : " << storedCount << std::endl;
    std::cout << "Lost     : " << MESSAGE_COUNT - storedCount << std::endl;

    SOCKET tcpClientSocket = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);

    if (tcpClientSocket == INVALID_SOCKET)
    {
        std::cerr << "TCP client socket failed" << std::endl;
        WSACleanup();
        return -1;
    }

    sockaddr_in tcpServerAddress;
    ZeroMemory(&tcpServerAddress, sizeof(tcpServerAddress));

    tcpServerAddress.sin_family = AF_INET;
    tcpServerAddress.sin_port = htons(TCP_PORT);
    inet_pton(AF_INET, "127.0.0.1", &tcpServerAddress.sin_addr);

    if (connect(
        tcpClientSocket,
        reinterpret_cast<sockaddr*>(&tcpServerAddress),
        sizeof(tcpServerAddress)) == SOCKET_ERROR)
    {
        std::cerr << "TCP connect failed: " << WSAGetLastError() << std::endl;
        closesocket(tcpClientSocket);
        WSACleanup();
        return -1;
    }

    int querySuccessCount = 0;
    int queryFailCount = 0;

    auto queryStartTime = std::chrono::high_resolution_clock::now();

    for (unsigned int offset = 0; offset < storedCount; ++offset)
    {
        TcpQueryRequest request;

        request.topic = htonl(TOPIC);
        request.offset = htonl(static_cast<uint32_t>(offset));

        if (sendAll(
            tcpClientSocket,
            reinterpret_cast<const char*>(&request),
            sizeof(request)) == false)
        {
            ++queryFailCount;
            break;
        }

        TcpQueryResponseHeader responseHeader;

        if (recvAll(
            tcpClientSocket,
            reinterpret_cast<char*>(&responseHeader),
            sizeof(responseHeader)) == false)
        {
            ++queryFailCount;
            break;
        }

        uint32_t resultCode = ntohl(responseHeader.resultCode);
        uint32_t payloadLength = ntohl(responseHeader.payloadLength);

        std::vector<char> responsePayload(payloadLength);

        if (payloadLength > 0)
        {
            if (recvAll(
                tcpClientSocket,
                responsePayload.data(),
                static_cast<int>(payloadLength)) == false)
            {
                ++queryFailCount;
                break;
            }
        }

        if (resultCode == MsgBroker::code_ok &&
            payloadLength == PAYLOAD_SIZE + sizeof(uint16_t) + sizeof(uint16_t))
        {
            ++querySuccessCount;
        }
        else
        {
            ++queryFailCount;
        }
    }

    auto queryEndTime = std::chrono::high_resolution_clock::now();

    double queryElapsedSec =
        std::chrono::duration<double>(queryEndTime - queryStartTime).count();

    closesocket(tcpClientSocket);
    WSACleanup();

    auto totalEndTime = std::chrono::high_resolution_clock::now();

    double totalElapsedSec =
        std::chrono::duration<double>(totalEndTime - totalStartTime).count();

    std::cout << "==============================" << std::endl;
    std::cout << "TCP Query Complete" << std::endl;
    std::cout << "Query Target: " << storedCount << std::endl;
    std::cout << "Query Success: " << querySuccessCount << std::endl;
    std::cout << "Query Fail: " << queryFailCount << std::endl;
    std::cout << "Query Time: " << queryElapsedSec << " sec" << std::endl;

    if (queryElapsedSec > 0.0)
    {
        std::cout << "Query Rate: " << querySuccessCount / queryElapsedSec << " msg/s" << std::endl;
    }

    std::cout << "==============================" << std::endl;
    std::cout << "Total Time: " << totalElapsedSec << " sec" << std::endl;
    std::cout << "==============================" << std::endl;

    return 0;
}