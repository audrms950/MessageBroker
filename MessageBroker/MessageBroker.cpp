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

static void sendTestMessages(int port, int messageCount, int payloadSize)
{
    WSADATA wsaData;

    if (WSAStartup(MAKEWORD(2, 2), &wsaData) != 0)
    {
        std::cerr << "Sender WSAStartup failed" << std::endl;
        return;
    }

    SOCKET senderSocket = socket(AF_INET, SOCK_DGRAM, IPPROTO_UDP);

    if (senderSocket == INVALID_SOCKET)
    {
        std::cerr << "Sender socket creation failed" << std::endl;
        WSACleanup();
        return;
    }

    sockaddr_in serverAddress;
    ZeroMemory(&serverAddress, sizeof(serverAddress));

    serverAddress.sin_family = AF_INET;
    serverAddress.sin_port = htons(port);
    inet_pton(AF_INET, "127.0.0.1", &serverAddress.sin_addr);

    const uint16_t MARK = 0xABCD;
    const uint16_t TOPIC = 1;

    std::vector<unsigned char> packet(sizeof(uint16_t) * 2 + payloadSize);

    std::memcpy(packet.data(), &MARK, sizeof(MARK));
    std::memcpy(packet.data() + sizeof(uint16_t), &TOPIC, sizeof(TOPIC));

    for (int i = 0; i < payloadSize; ++i)
    {
        packet[sizeof(uint16_t) * 2 + i] =
            static_cast<unsigned char>('A' + (i % 26));
    }

    for (int i = 0; i < messageCount; ++i)
    {
        int sendResult = sendto(
            senderSocket,
            reinterpret_cast<const char*>(packet.data()),
            static_cast<int>(packet.size()),
            0,
            reinterpret_cast<sockaddr*>(&serverAddress),
            sizeof(serverAddress));

        if (sendResult == SOCKET_ERROR)
        {
            std::cerr << "sendto failed: "
                << WSAGetLastError()
                << std::endl;
            break;
        }
        else
        {
            if ((i + 1) % 100 == 0)
            {
                std::this_thread::sleep_for(std::chrono::microseconds(1));
            }
        }
        
    }

    closesocket(senderSocket);
    WSACleanup();
}

int main()
{
    const int PORT = 9000;
    int MESSAGE_COUNT = 40000;
    const int PAYLOAD_SIZE = 50;

    try
    {
        MsgBroker broker;
        ProducerMessage receiver(PORT);

        receiver.binding();

        std::thread recvThread([&]()
            {
                receiver.recvBuf(&broker);
            });

        auto startTime = std::chrono::high_resolution_clock::now();

        sendTestMessages(PORT, MESSAGE_COUNT, PAYLOAD_SIZE);

        while (broker.getStoredMessageCount() < MESSAGE_COUNT)
        {
            std::this_thread::yield();
        }

        auto endTime = std::chrono::high_resolution_clock::now();

        auto elapsedMicroSec =
            std::chrono::duration_cast<std::chrono::microseconds>(
                endTime - startTime).count();

        double elapsedSec = elapsedMicroSec / 1000000.0;


        MESSAGE_COUNT = broker.getStoredMessageCount();
        size_t totalBytes =
            static_cast<size_t>(MESSAGE_COUNT) *
            static_cast<size_t>(PAYLOAD_SIZE + sizeof(uint16_t) * 2);

        double totalMb = totalBytes / 1024.0 / 1024.0;
        double mbPerSec = totalMb / elapsedSec;
        double msgPerSec = MESSAGE_COUNT / elapsedSec;

        std::cout << "==============================" << std::endl;
        std::cout << "Performance Test Result" << std::endl;
        std::cout << "Message Count: " << MESSAGE_COUNT << std::endl;
        std::cout << "Payload Size: " << PAYLOAD_SIZE << " bytes" << std::endl;
        std::cout << "Packet Size: " << PAYLOAD_SIZE + sizeof(uint16_t) * 2 << " bytes" << std::endl;
        std::cout << "Total Size: " << totalMb << " MB" << std::endl;
        std::cout << "Elapsed Time: " << elapsedSec << " sec" << std::endl;
        std::cout << "Throughput: " << mbPerSec << " MB/s" << std::endl;
        std::cout << "Message Rate: " << msgPerSec << " msg/s" << std::endl;
        std::cout << "==============================" << std::endl;

        std::vector<unsigned char> outBuffer;

        MsgBroker::code result = broker.getMessage(1, 0, outBuffer);

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

        recvThread.detach();

        std::cout << "Test finished" << std::endl;
    }
    catch (const std::exception& ex)
    {
        std::cerr << ex.what() << std::endl;
        return -1;
    }

    return 0;
}