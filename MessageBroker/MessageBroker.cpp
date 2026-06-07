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
#include <memory>
#include <atomic>

#pragma comment(lib, "Ws2_32.lib")

struct SendHeader
{
    uint16_t mark;
    uint16_t topic;
};

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

void sendUdpMessages(
    int udpPort,
    int topic,
    int messageCount,
    int payloadSize,
    std::atomic<int>& totalSendSuccessCount,
    std::atomic<int>& totalSendFailCount)
{
    SOCKET udpClientSocket = socket(AF_INET, SOCK_DGRAM, IPPROTO_UDP);

    if (udpClientSocket == INVALID_SOCKET)
    {
        totalSendFailCount.fetch_add(messageCount, std::memory_order_relaxed);
        return;
    }

    sockaddr_in udpServerAddress;
    ZeroMemory(&udpServerAddress, sizeof(udpServerAddress));

    udpServerAddress.sin_family = AF_INET;
    udpServerAddress.sin_port = htons(static_cast<u_short>(udpPort));
    inet_pton(AF_INET, "127.0.0.1", &udpServerAddress.sin_addr);

    std::vector<char> packet(sizeof(SendHeader) + payloadSize);

    for (int i = 0; i < messageCount; ++i)
    {
        SendHeader header;

        header.mark = htons(static_cast<uint16_t>(i & 0xFFFF));
        header.topic = htons(static_cast<uint16_t>(topic));

        std::memcpy(packet.data(), &header, sizeof(header));

        std::memset(
            packet.data() + sizeof(header),
            'A' + (i % 26),
            payloadSize);

        int sentBytes = sendto(
            udpClientSocket,
            packet.data(),
            static_cast<int>(packet.size()),
            0,
            reinterpret_cast<sockaddr*>(&udpServerAddress),
            sizeof(udpServerAddress));

        if (sentBytes == SOCKET_ERROR)
        {
            totalSendFailCount.fetch_add(1, std::memory_order_relaxed);
        }
        else
        {
            totalSendSuccessCount.fetch_add(1, std::memory_order_relaxed);
        }
    }

    closesocket(udpClientSocket);
}

int runTcpQueryTest(
    int tcpPort,
    int topic,
    unsigned int queryTarget,
    int expectedPayloadSize)
{
    SOCKET tcpClientSocket = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);

    if (tcpClientSocket == INVALID_SOCKET)
    {
        std::cerr << "TCP client socket failed" << std::endl;
        return 0;
    }

    sockaddr_in tcpServerAddress;
    ZeroMemory(&tcpServerAddress, sizeof(tcpServerAddress));

    tcpServerAddress.sin_family = AF_INET;
    tcpServerAddress.sin_port = htons(static_cast<u_short>(tcpPort));
    inet_pton(AF_INET, "127.0.0.1", &tcpServerAddress.sin_addr);

    if (connect(
        tcpClientSocket,
        reinterpret_cast<sockaddr*>(&tcpServerAddress),
        sizeof(tcpServerAddress)) == SOCKET_ERROR)
    {
        std::cerr << "TCP connect failed: " << WSAGetLastError() << std::endl;
        closesocket(tcpClientSocket);
        return 0;
    }

    int querySuccessCount = 0;

    for (unsigned int offset = 0; offset < queryTarget; ++offset)
    {
        TcpQueryRequest request;

        request.topic = htonl(static_cast<uint32_t>(topic));
        request.offset = htonl(offset);

        if (sendAll(
            tcpClientSocket,
            reinterpret_cast<const char*>(&request),
            sizeof(request)) == false)
        {
            break;
        }

        TcpQueryResponseHeader responseHeader;

        if (recvAll(
            tcpClientSocket,
            reinterpret_cast<char*>(&responseHeader),
            sizeof(responseHeader)) == false)
        {
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
                break;
            }
        }

        if (resultCode == MsgBroker::code_ok &&
            payloadLength == expectedPayloadSize)
        {
            ++querySuccessCount;
        }
    }

    closesocket(tcpClientSocket);

    return querySuccessCount;
}

unsigned int verifyTopic(
    int tcpPort,
    int topic,
    unsigned int expectedCount,
    int expectedPayloadSize)
{
    return runTcpQueryTest(
        tcpPort,
        topic,
        expectedCount,
        expectedPayloadSize);
}


int main()
{
    constexpr int BASE_UDP_PORT = 9000;
    constexpr int TCP_PORT = 9100;
    constexpr int TOPIC = 5;

    constexpr int RECEIVER_COUNT = 5;
    constexpr int TOTAL_MESSAGE_COUNT = 1000000;
    constexpr int MESSAGE_COUNT_PER_RECEIVER = TOTAL_MESSAGE_COUNT / RECEIVER_COUNT;
    constexpr int PAYLOAD_SIZE = 1024;
    constexpr int MESSAGE_COUNT_PER_TOPIC =
        TOTAL_MESSAGE_COUNT / RECEIVER_COUNT;

    constexpr auto TEST_TIMEOUT = std::chrono::seconds(10);

    WSADATA wsaData;

    if (WSAStartup(MAKEWORD(2, 2), &wsaData) != 0)
    {
        std::cerr << "WSAStartup failed" << std::endl;
        return -1;
    }

    MsgBroker broker;

    std::vector<std::unique_ptr<ProducerReceiver>> receivers;
    receivers.reserve(RECEIVER_COUNT);

    for (int i = 0; i < RECEIVER_COUNT; ++i)
    {
        int topic = i + 1;
        int udpPort = BASE_UDP_PORT + i;

        auto receiver = std::make_unique<ProducerReceiver>(
            topic,
            udpPort,
            &broker);

        receiver->binding();
        receiver->start();

        receivers.push_back(std::move(receiver));
    }

    TcpQueryServer queryServer(TCP_PORT, &broker);

    if (queryServer.binding() == false)
    {
        std::cerr << "TCP Query Server binding failed" << std::endl;
        WSACleanup();
        return -1;
    }

    queryServer.start();

    std::this_thread::sleep_for(std::chrono::milliseconds(500));

    std::atomic<int> totalSendSuccessCount = 0;
    std::atomic<int> totalSendFailCount = 0;

    std::vector<std::thread> senderThreads;
    senderThreads.reserve(RECEIVER_COUNT);

    auto totalStartTime = std::chrono::high_resolution_clock::now();
    auto sendStartTime = std::chrono::high_resolution_clock::now();

    for (int i = 0; i < RECEIVER_COUNT; ++i)
    {
        int topic = i + 1;
        int udpPort = BASE_UDP_PORT + i;

        senderThreads.emplace_back(
            sendUdpMessages,
            udpPort,
            topic,
            MESSAGE_COUNT_PER_TOPIC,
            PAYLOAD_SIZE,
            std::ref(totalSendSuccessCount),
            std::ref(totalSendFailCount));
    }

    for (auto& thread : senderThreads)
    {
        if (thread.joinable())
        {
            thread.join();
        }
    }

    auto sendEndTime = std::chrono::high_resolution_clock::now();

    double sendElapsedSec =
        std::chrono::duration<double>(sendEndTime - sendStartTime).count();

    int sendSuccessCount = totalSendSuccessCount.load(std::memory_order_relaxed);
    int sendFailCount = totalSendFailCount.load(std::memory_order_relaxed);

    std::cout << "==============================" << std::endl;
    std::cout << "Multi UDP Send Complete" << std::endl;
    std::cout << "Receiver Count: " << RECEIVER_COUNT << std::endl;
    std::cout << "Send Success: " << sendSuccessCount << std::endl;
    std::cout << "Send Fail: " << sendFailCount << std::endl;
    std::cout << "Send Time: " << sendElapsedSec << " sec" << std::endl;
    std::cout << "Send Rate: " << sendSuccessCount / sendElapsedSec << " msg/s" << std::endl;

    auto storeTimeoutStartTime = std::chrono::steady_clock::now();

    while (broker.getStoredMessageCount() < static_cast<unsigned int>(sendSuccessCount) &&
        std::chrono::steady_clock::now() - storeTimeoutStartTime < TEST_TIMEOUT)
    {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }

    unsigned int storedCount = broker.getStoredMessageCount();

    std::cout << "==============================" << std::endl;

    if (storedCount == static_cast<unsigned int>(sendSuccessCount))
    {
        std::cout << "All received messages stored successfully." << std::endl;
    }
    else
    {
        std::cout << "Timeout reached." << std::endl;
    }

    std::cout << "Expected : " << sendSuccessCount << std::endl;
    std::cout << "Stored   : " << storedCount << std::endl;
    std::cout << "Lost     : " << sendSuccessCount - storedCount << std::endl;

    int expectedPayloadSize =
        PAYLOAD_SIZE + sizeof(uint16_t) + sizeof(uint16_t);


    WSACleanup();

    std::cout
        << "=============================="
        << std::endl;

    unsigned int totalQuerySuccess = 0;
    unsigned int totalQueryFail = 0;

    for (int topic = 1;
        topic <= RECEIVER_COUNT;
        ++topic)
    {
        auto queryStartTime =
            std::chrono::high_resolution_clock::now();

        unsigned int successCount =
            verifyTopic(
                TCP_PORT,
                topic,
                MESSAGE_COUNT_PER_TOPIC,
                expectedPayloadSize);

        auto queryEndTime =
            std::chrono::high_resolution_clock::now();

        double queryElapsedSec =
            std::chrono::duration<double>(
                queryEndTime - queryStartTime).count();

        unsigned int failCount =
            MESSAGE_COUNT_PER_TOPIC - successCount;

        totalQuerySuccess += successCount;
        totalQueryFail += failCount;

        std::cout
            << "Topic "
            << topic
            << std::endl;

        std::cout
            << "  Success : "
            << successCount
            << std::endl;

        std::cout
            << "  Fail    : "
            << failCount
            << std::endl;

        std::cout
            << "  Rate    : "
            << successCount / queryElapsedSec
            << " msg/s"
            << std::endl;

        std::cout
            << "------------------------------"
            << std::endl;
    }

    std::cout
        << "Total Query Success : "
        << totalQuerySuccess
        << std::endl;

    std::cout
        << "Total Query Fail : "
        << totalQueryFail
        << std::endl;

    return 0;
}