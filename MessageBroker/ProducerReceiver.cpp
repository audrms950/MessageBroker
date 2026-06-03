#include "ProducerReceiver.h"

#include <iostream>
#include <stdexcept>
#include <string>

ProducerReceiver::ProducerReceiver(int port, MsgBroker* broker) : port(port), socketHandle(INVALID_SOCKET), broker(broker)
{
    static_assert(sizeof(MessageHeader) == 6, "MessageHeader size must be 6 bytes");

    for (int i = 0; i < INITIAL_BUFFER_COUNT; ++i)
    {
        allocatedBufferCount.fetch_add(1, std::memory_order_relaxed);
		queue_ready.emplace(std::make_unique<ReceiveBuffer>());
    }

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

ProducerReceiver::~ProducerReceiver()
{
    running = false;
    bufferCondition.notify_all();

    if (socketHandle != INVALID_SOCKET)
    {
        closesocket(socketHandle);
        socketHandle = INVALID_SOCKET;
    }

    for (auto& thread : workerThreads)
    {
        if (thread.joinable())
        {
            thread.join();
        }
    }

    WSACleanup();

	std::cout << queue_ready.size() << " buffers in ready queue, "
		<< queue_process.size() << " buffers in process queue at shutdown." << std::endl;
}

ProducerReceiver& ProducerReceiver::binding()
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

void ProducerReceiver::start()
{
    std::thread receiveThread(&ProducerReceiver::recvBuf, this, 1);
    std::thread processThread(&ProducerReceiver::workerThread, this);

    workerThreads.push_back(std::move(receiveThread));
    workerThreads.push_back(std::move(processThread));
}

void ProducerReceiver::recvBuf(int topic)
{
    unsigned long long totalRecvCount = 0;
    sockaddr_in clientAddress;
    int clientAddressSize = sizeof(clientAddress);
    unsigned int curOffset = sizeof(uint16_t);
    running = true;

    socketoption();

    std::unique_ptr<ReceiveBuffer> recvOb = getReadyBuffer();
    if (recvOb == nullptr)
    {
        std::cerr << "No initial receive buffer." << std::endl;
        return;
    }

    auto flushBuffer = [&]() -> bool
        {
            if (curOffset <= sizeof(uint16_t)) return true;
            if (recvOb == nullptr) return false;

            /* curOffset은 항상 2바이트 앞서있기 때문에 오프셋 조정 후 추가 */
            recvOb->usedSize = curOffset - sizeof(uint16_t) ;
            pushProcessBuffer(std::move(recvOb));
            recvOb = getReadyBuffer();

            if (recvOb == nullptr)
            {
                std::cerr << "No available buffer. Dropping incoming data." << std::endl;
                return false;
            }

            curOffset = sizeof(uint16_t);
            return true;
        };


    while (socketHandle != INVALID_SOCKET && running.load())
    {
        clientAddressSize = sizeof(clientAddress);

        unsigned int writableSize = BLOCK_BUFFER_SIZE - curOffset;

        if (writableSize < MAX_PACKET_SIZE + sizeof(uint16_t))
        {
            running = flushBuffer();
            continue;
        }

        int bytesReceived = recvfrom( socketHandle, recvOb->buffer.data() + curOffset, MAX_PACKET_SIZE, 0, reinterpret_cast<sockaddr*>(&clientAddress), &clientAddressSize);
        
        if (bytesReceived == SOCKET_ERROR)
        {
            int errorCode = WSAGetLastError();

            if (errorCode == WSAETIMEDOUT) running = flushBuffer();
            else std::cerr << "recvfrom failed: " << errorCode << std::endl;
            
            continue;
        }
        /* 메시지 헤더보다 작은 데이터는 무시 */
        if (bytesReceived <= static_cast<int>(sizeof(MessageHeader))) continue;  

        /* 메시지 길이 필드에 실제 수신된 바이트 수를 기록 (헤더 포함) */
		uint16_t messageLength = static_cast<uint16_t>(bytesReceived); 
        std::memcpy(recvOb->buffer.data() + curOffset - sizeof(uint16_t), &messageLength, sizeof(messageLength));
        ++totalRecvCount;

        curOffset += bytesReceived + sizeof(uint16_t);
		recvOb->recvCount++;

		if (recvOb->recvCount >= FLUSH_MESSAGE_COUNT)
		{
			running = flushBuffer();
		}
    }

    std::cout
        << "[recv] packet count : "
        << totalRecvCount
        << std::endl;

    flushBuffer();
}

bool ProducerReceiver::hasProcessBuffer()
{
    std::lock_guard<std::mutex> lock(bufferMutex);
    return queue_process.empty() == false;
}

void ProducerReceiver::workerThread()
{
    while (true)
    {
        std::unique_ptr<ReceiveBuffer> recvOb;

        {
            std::unique_lock<std::mutex> lock(bufferMutex);

            bufferCondition.wait(lock, [&]()
                {
                    return queue_process.empty() == false || running.load() == false;
                });

            if (queue_process.empty())
            {
                if (running.load() == false)
                {
                    break;
                }

                continue;
            }

            std::cout << queue_process.size() << " buffers in process queue." << std::endl;

            recvOb = std::move(queue_process.front());
            queue_process.pop();
        }

        broker->pushBatch(1, recvOb->buffer, recvOb->usedSize);

        pushReadyBuffer(std::move(recvOb));
    }
}

void ProducerReceiver::socketoption()
{
    int recvBufferSize = 1024 * 1024 * 64;

    setsockopt(
        socketHandle,
        SOL_SOCKET,
        SO_RCVBUF,
        reinterpret_cast<const char*>(&recvBufferSize),
        sizeof(recvBufferSize));

    setsockopt(
        socketHandle,
        SOL_SOCKET,
        SO_RCVTIMEO,
        reinterpret_cast<const char*>(&FLUSH_TIMEOUT_MS),
        sizeof(FLUSH_TIMEOUT_MS));
}

std::unique_ptr<ProducerReceiver::ReceiveBuffer> ProducerReceiver::getReadyBuffer()
{
    std::lock_guard<std::mutex> lock(bufferMutex);

    if (queue_ready.empty())
    {
        if (allocatedBufferCount.load(std::memory_order_relaxed) >= MAX_BUFFER_COUNT)
        {
            return nullptr;
        }

        auto buffer = std::make_unique<ProducerReceiver::ReceiveBuffer>();
        allocatedBufferCount.fetch_add(1, std::memory_order_relaxed);
        return buffer;
    }

    std::unique_ptr<ReceiveBuffer> buffer = std::move(queue_ready.front());
    queue_ready.pop();

    return buffer;
}

void ProducerReceiver::pushReadyBuffer(std::unique_ptr<ReceiveBuffer> buffer)
{
    if (buffer == nullptr)
    {
        return;
    }

    buffer->reset();

    {
        std::lock_guard<std::mutex> lock(bufferMutex);
        queue_ready.push(std::move(buffer));
    }
}

void ProducerReceiver::pushProcessBuffer(std::unique_ptr<ProducerReceiver::ReceiveBuffer> buffer)
{
    if (buffer == nullptr)
    {
        return;
    }

    {
        std::lock_guard<std::mutex> lock(bufferMutex);
        queue_process.push(std::move(buffer));
    }

    bufferCondition.notify_one();
}

std::unique_ptr<ProducerReceiver::ReceiveBuffer> ProducerReceiver::getProcessBuffer()
{
    std::unique_lock<std::mutex> lock(bufferMutex);

    bufferCondition.wait(lock, [&]()
        {
            return !queue_process.empty() || running == false;
        });

    if (queue_process.empty())
    {
        return nullptr;
    }

    std::unique_ptr<ReceiveBuffer> buffer = std::move(queue_process.front());
    queue_process.pop();

    return buffer;
}
