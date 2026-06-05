#include "ProducerReceiver.h"
#include "MsgBroker.h"

#include <iostream>
#include <thread>
#include <chrono>
#include <vector>
#include <atomic>
#include <winsock2.h>
#include <ws2tcpip.h>

#pragma comment(lib, "Ws2_32.lib")

#include <iostream>
#include <vector>
#include <chrono>
#include <cstring>
#include <cstdint>

void readWorker(
    MsgBroker* broker,
    int topic,
    int messageCount,
    int threadIndex,
    int threadCount,
    std::atomic<unsigned int>& successCount,
    std::atomic<unsigned int>& failCount);

void engineTest();

#include "ProducerReceiver.h"
#include "MsgBroker.h"

#include <iostream>
#include <thread>
#include <chrono>
#include <vector>
#include <atomic>
#include <cstring>
#include <cstdint>
#include <winsock2.h>
#include <ws2tcpip.h>

#pragma comment(lib, "Ws2_32.lib")

void runBrokerOnlyPerformanceTest();
void runUdpReceiverPerformanceTest();

int main()
{
    runBrokerOnlyPerformanceTest();
    runUdpReceiverPerformanceTest();

    return 0;
}

void runBrokerOnlyPerformanceTest()
{
    constexpr int TOPIC = 1;
    constexpr int MESSAGE_COUNT = 1000000;
    constexpr int PAYLOAD_SIZE = 1024;
    constexpr int BATCH_MESSAGE_COUNT = 500;

    MsgBroker broker;

    const unsigned int PACKET_SIZE =
        sizeof(uint16_t) + PAYLOAD_SIZE + sizeof(uint16_t);

    const unsigned int BATCH_BUFFER_SIZE =
        PACKET_SIZE * BATCH_MESSAGE_COUNT;

    std::vector<char> batchBuffer(BATCH_BUFFER_SIZE);

    unsigned int curOffset = 0;

    for (int i = 0; i < BATCH_MESSAGE_COUNT; ++i)
    {
        uint16_t messageLength =
            static_cast<uint16_t>(PAYLOAD_SIZE + sizeof(uint16_t));

        std::memcpy(
            batchBuffer.data() + curOffset,
            &messageLength,
            sizeof(messageLength));

        curOffset += sizeof(messageLength);

        uint16_t mark = static_cast<uint16_t>(i);

        std::memcpy(
            batchBuffer.data() + curOffset,
            &mark,
            sizeof(mark));

        curOffset += sizeof(mark);

        std::memset(
            batchBuffer.data() + curOffset,
            'A',
            PAYLOAD_SIZE);

        curOffset += PAYLOAD_SIZE;
    }

    auto startTime =
        std::chrono::high_resolution_clock::now();

    int sentCount = 0;

    while (sentCount < MESSAGE_COUNT)
    {
        broker.pushBatch(
            TOPIC,
            batchBuffer,
            curOffset);

        sentCount += BATCH_MESSAGE_COUNT;
    }

    auto endTime =
        std::chrono::high_resolution_clock::now();

    double elapsedSec =
        std::chrono::duration<double>(
            endTime - startTime).count();

    double totalMb =
        static_cast<double>(MESSAGE_COUNT * PACKET_SIZE) / 1024.0 / 1024.0;

    double mbPerSec = totalMb / elapsedSec;
    double msgPerSec =
        static_cast<double>(MESSAGE_COUNT) / elapsedSec;

    std::cout << "==============================" << std::endl;
    std::cout << "Broker Only Performance Test" << std::endl;
    std::cout << "Message Count: " << MESSAGE_COUNT << std::endl;
    std::cout << "Payload Size: " << PAYLOAD_SIZE << " bytes" << std::endl;
    std::cout << "Packet Size: " << PACKET_SIZE << " bytes" << std::endl;
    std::cout << "Batch Count: " << BATCH_MESSAGE_COUNT << std::endl;
    std::cout << "Total Size: " << totalMb << " MB" << std::endl;
    std::cout << "Elapsed Time: " << elapsedSec << " sec" << std::endl;
    std::cout << "Throughput: " << mbPerSec << " MB/s" << std::endl;
    std::cout << "Message Rate: " << msgPerSec << " msg/s" << std::endl;
    std::cout << "Stored Count: " << broker.getStoredMessageCount() << std::endl;
    std::cout << "==============================" << std::endl;

    std::vector<unsigned char> outBuffer;

    MsgBroker::code result =
        broker.getMessage(
            TOPIC,
            MESSAGE_COUNT - 1,
            outBuffer);

    if (result == MsgBroker::code_ok)
    {
        std::cout
            << "Broker getMessage success. size: "
            << outBuffer.size()
            << std::endl;
    }
    else
    {
        std::cout
            << "Broker getMessage failed. code: "
            << result
            << std::endl;
    }
}

void runUdpReceiverPerformanceTest()
{
    constexpr int PORT = 9000;
    constexpr int TOPIC = 1;
    constexpr int MESSAGE_COUNT = 100000;
    constexpr int PAYLOAD_SIZE = 1024;

    MsgBroker broker;

    ProducerReceiver receiver(1, PORT, &broker);

    receiver
        .binding();

    receiver.start();

    WSADATA wsaData;

    if (WSAStartup(MAKEWORD(2, 2), &wsaData) != 0)
    {
        std::cerr << "Client WSAStartup failed" << std::endl;
        return;
    }

    SOCKET clientSocket =
        socket(AF_INET, SOCK_DGRAM, IPPROTO_UDP);

    if (clientSocket == INVALID_SOCKET)
    {
        std::cerr << "Client socket creation failed" << std::endl;
        WSACleanup();
        return;
    }

    sockaddr_in serverAddress;
    ZeroMemory(&serverAddress, sizeof(serverAddress));

    serverAddress.sin_family = AF_INET;
    serverAddress.sin_port = htons(PORT);
    inet_pton(AF_INET, "127.0.0.1", &serverAddress.sin_addr);

    std::vector<char> sendBuffer(PAYLOAD_SIZE);

    ProducerReceiver::MessageHeader header;
    header.length =
        sizeof(header.mark) +
        sizeof(header.topic) +
        static_cast<uint16_t>(sendBuffer.size());

    header.mark = 1;
    header.topic = TOPIC;

    std::vector<char> packet(
        sizeof(header) + sendBuffer.size());

    std::memcpy(
        packet.data(),
        &header,
        sizeof(header));

    std::memset(
        packet.data() + sizeof(header),
        'A',
        sendBuffer.size());

    int sendSuccessCount = 0;
    int sendFailCount = 0;

    auto startTime =
        std::chrono::high_resolution_clock::now();

    for (int i = 0; i < MESSAGE_COUNT; ++i)
    {
        int sentBytes =
            sendto(
                clientSocket,
                packet.data(),
                static_cast<int>(packet.size()),
                0,
                reinterpret_cast<sockaddr*>(&serverAddress),
                sizeof(serverAddress));

        if (sentBytes == SOCKET_ERROR)
        {
            ++sendFailCount;
        }
        else
        {
            ++sendSuccessCount;
        }
    }

    auto endTime =
        std::chrono::high_resolution_clock::now();

    double elapsedSec =
        std::chrono::duration<double>(
            endTime - startTime).count();

    double totalMb =
        static_cast<double>(MESSAGE_COUNT * packet.size()) / 1024.0 / 1024.0;

    std::cout << "==============================" << std::endl;
    std::cout << "UDP Receiver Performance Test" << std::endl;
    std::cout << "Message Count: " << MESSAGE_COUNT << std::endl;
    std::cout << "Payload Size: " << PAYLOAD_SIZE << " bytes" << std::endl;
    std::cout << "Packet Size: " << packet.size() << " bytes" << std::endl;
    std::cout << "Send Success: " << sendSuccessCount << std::endl;
    std::cout << "Send Fail: " << sendFailCount << std::endl;
    std::cout << "Send Time: " << elapsedSec << " sec" << std::endl;
    std::cout << "Send Rate: "
        << static_cast<double>(sendSuccessCount) / elapsedSec
        << " msg/s" << std::endl;
    std::cout << "Send Throughput: "
        << totalMb / elapsedSec
        << " MB/s" << std::endl;

    std::this_thread::sleep_for(
        std::chrono::seconds(3));

    std::cout
        << "Broker Stored: "
        << broker.getStoredMessageCount()
        << std::endl;

    std::vector<unsigned char> outBuffer;

    unsigned int storedCount =
        broker.getStoredMessageCount();

    if (storedCount > 0)
    {
        auto result =
            broker.getMessage(
                TOPIC,
                storedCount - 1,
                outBuffer);

        if (result == MsgBroker::code_ok)
        {
            std::cout
                << "Last Message Size: "
                << outBuffer.size()
                << std::endl;
        }
    }

    std::cout << "==============================" << std::endl;

    closesocket(clientSocket);
    WSACleanup();
}










void engineTest()
{
    constexpr int TOPIC = 1;
    constexpr int MESSAGE_COUNT = 100000;
    constexpr int PAYLOAD_SIZE = 1024;
    constexpr int BATCH_MESSAGE_COUNT = 500;

    MsgBroker broker;

    const unsigned int PACKET_SIZE =
        sizeof(uint16_t) + PAYLOAD_SIZE + sizeof(uint16_t);

    const unsigned int BATCH_BUFFER_SIZE =
        PACKET_SIZE * BATCH_MESSAGE_COUNT;

    std::vector<char> batchBuffer(BATCH_BUFFER_SIZE);

    unsigned int curOffset = 0;

    for (int i = 0; i < BATCH_MESSAGE_COUNT; ++i)
    {
        uint16_t messageLength = static_cast<uint16_t>(PAYLOAD_SIZE + sizeof(uint16_t));

        std::memcpy(
            batchBuffer.data() + curOffset,
            &messageLength,
            sizeof(messageLength));

        curOffset += sizeof(messageLength);

        uint16_t mark = static_cast<uint16_t>(i);

        std::memcpy(
            batchBuffer.data() + curOffset,
            &mark,
            sizeof(mark));

        curOffset += sizeof(mark);

        std::memset(
            batchBuffer.data() + curOffset,
            'A',
            PAYLOAD_SIZE);

        curOffset += PAYLOAD_SIZE;
    }

    auto startTime = std::chrono::high_resolution_clock::now();

    int sentCount = 0;

    while (sentCount < MESSAGE_COUNT)
    {
        broker.pushBatch(TOPIC, batchBuffer, curOffset);
        sentCount += BATCH_MESSAGE_COUNT;
    }

    auto endTime = std::chrono::high_resolution_clock::now();

    double elapsedSec =
        std::chrono::duration<double>(endTime - startTime).count();

    double totalMb =
        static_cast<double>(MESSAGE_COUNT * PACKET_SIZE) / 1024.0 / 1024.0;

    double mbPerSec = totalMb / elapsedSec;
    double msgPerSec = MESSAGE_COUNT / elapsedSec;

    std::cout << "==============================" << std::endl;
    std::cout << "Broker Only Performance Test" << std::endl;
    std::cout << "Message Count: " << MESSAGE_COUNT << std::endl;
    std::cout << "Payload Size: " << PAYLOAD_SIZE << " bytes" << std::endl;
    std::cout << "Packet Size: " << PACKET_SIZE << " bytes" << std::endl;
    std::cout << "Batch Count: " << BATCH_MESSAGE_COUNT << std::endl;
    std::cout << "Total Size: " << totalMb << " MB" << std::endl;
    std::cout << "Elapsed Time: " << elapsedSec << " sec" << std::endl;
    std::cout << "Throughput: " << mbPerSec << " MB/s" << std::endl;
    std::cout << "Message Rate: " << msgPerSec << " msg/s" << std::endl;
    std::cout << "Stored Count: " << broker.getStoredMessageCount() << std::endl;
    std::cout << "==============================" << std::endl;

    std::vector<unsigned char> outBuffer;

    MsgBroker::code result = broker.getMessage(
        TOPIC,
        MESSAGE_COUNT - 1,
        outBuffer);

    if (result == MsgBroker::code_ok)
    {
        std::cout << "Broker getMessage success. size: "
            << outBuffer.size()
            << std::endl;
    }
    else
    {
        std::cout << "Broker getMessage failed. code: "
            << result
            << std::endl;
    }


    constexpr int READ_THREAD_COUNT = 8;

    std::atomic<unsigned int> readSuccessCount = 0;
    std::atomic<unsigned int> readFailCount = 0;

    std::vector<std::thread> readThreads;

    auto readStartTime = std::chrono::high_resolution_clock::now();

    for (int i = 0; i < READ_THREAD_COUNT; ++i)
    {
        readThreads.emplace_back(
            readWorker,
            &broker,
            TOPIC,
            MESSAGE_COUNT,
            i,
            READ_THREAD_COUNT,
            std::ref(readSuccessCount),
            std::ref(readFailCount));
    }

    for (auto& thread : readThreads)
    {
        thread.join();
    }

    auto readEndTime = std::chrono::high_resolution_clock::now();

    double readElapsedSec =
        std::chrono::duration<double>(readEndTime - readStartTime).count();

    double readMsgPerSec =
        static_cast<double>(readSuccessCount.load(std::memory_order_relaxed)) / readElapsedSec;

    std::cout << "==============================" << std::endl;
    std::cout << "Broker Read Performance Test" << std::endl;
    std::cout << "Read Thread Count: " << READ_THREAD_COUNT << std::endl;
    std::cout << "Read Success Count: " << readSuccessCount.load() << std::endl;
    std::cout << "Read Fail Count: " << readFailCount.load() << std::endl;
    std::cout << "Elapsed Time: " << readElapsedSec << " sec" << std::endl;
    std::cout << "Read Message Rate: " << readMsgPerSec << " msg/s" << std::endl;
    std::cout << "==============================" << std::endl;
}



void readWorker(
    MsgBroker* broker,
    int topic,
    int messageCount,
    int threadIndex,
    int threadCount,
    std::atomic<unsigned int>& successCount,
    std::atomic<unsigned int>& failCount)
{
    std::vector<unsigned char> outBuffer;

    for (int offset = threadIndex; offset < messageCount; offset += threadCount)
    {
        MsgBroker::code result = broker->getMessage(topic, offset, outBuffer);

        if (result == MsgBroker::code_ok)
        {
            successCount.fetch_add(1, std::memory_order_relaxed);
        }
        else
        {
            failCount.fetch_add(1, std::memory_order_relaxed);
        }
    }
}