#pragma once
#pragma comment(lib, "Ws2_32.lib")

#include <winsock2.h>
#include "MsgBroker.h"
#include <queue>
#include <thread>

class ProducerReceiver
{
private: 
    static constexpr int INITIAL_BUFFER_COUNT = 2; /* 버퍼 풀 처리를 위해 최소 2개 보장 */
    static constexpr int MAX_BUFFER_COUNT = 32;
    static constexpr int BLOCK_BUFFER_SIZE = 1024 * 1024 * 8;

    static constexpr int FLUSH_MESSAGE_COUNT = 1000;
    static constexpr int FLUSH_TIMEOUT_MS = 10;

    static constexpr int MAX_DATAGRAM_SIZE = 1024 * 64;
	static constexpr int MAX_PACKET_SIZE = 1024 * 8; 

public:
    struct MessageHeader
    {
        uint16_t length; /* 메시지 길이 */
        uint16_t mark;   /* 메시지 구분자 */
        uint16_t topic;  /* 메시지 토픽 */
    };

    struct ReceiveBuffer
    {
        std::vector<char> buffer = std::vector<char>(BLOCK_BUFFER_SIZE);
        unsigned int usedSize = 0;
        unsigned int recvCount = 0;

        void reset()
        {
            usedSize = 0;
            recvCount = 0;
        }
    };
private:/* 통신관련 */
    SOCKET socketHandle = INVALID_SOCKET;
    const unsigned int port;
    const unsigned int topic;
private: 
    MsgBroker* broker;

	std::mutex bufferMutex;
    std::condition_variable bufferCondition;
    std::queue<std::unique_ptr<ReceiveBuffer>> queue_ready;
    std::queue<std::unique_ptr<ReceiveBuffer>> queue_process;
    
    std::atomic<bool> running = false;
    std::atomic<int> allocatedBufferCount = 0;

	std::vector<std::thread> runningThreads;
public:
    explicit ProducerReceiver(unsigned int topic, unsigned int port, MsgBroker* broker);
    ~ProducerReceiver();
public:
    ProducerReceiver& binding();
	void start();

private:
    void th_recv(int topic);
	void th_worker();

private: /* 통신 프리미티브 */
	void socketoption();

private: /* 큐 프리미티브 */

    bool hasProcessBuffer();

    /* 프로세스 큐 */
    std::unique_ptr<ReceiveBuffer> getProcessBuffer();
    void pushProcessBuffer(std::unique_ptr<ReceiveBuffer> buffer);
    
    /* 준비 큐 */
    std::unique_ptr<ReceiveBuffer> getReadyBuffer(); /* 버퍼 없으면 동적으로 생성 (최대 MAX_BUFFER_COUNT 개수만큼) */
	void pushReadyBuffer(std::unique_ptr<ReceiveBuffer> buffer);

};