#pragma once
#pragma comment(lib, "Ws2_32.lib")

#include "MsgBroker.h"

#include <winsock2.h>
#include <ws2tcpip.h>
#include <thread>
#include <vector>
#include <atomic>

class TcpQueryServer
{
private:
	static constexpr int BACKLOG_COUNT = 64; // SOMAXCONN로 최대 대기 허용이긴 한데, 시스템에 따라 다르므로 명시적으로 설정
    static constexpr int RECV_BUFFER_SIZE = 1024;
private:
    unsigned int port = 0;
    SOCKET listen_scoket = INVALID_SOCKET;
    MsgBroker* broker = nullptr;

    std::atomic<bool> running = false;
    std::thread accept_thread;
    std::vector<std::thread> client_thread;


public:
    struct QueryRequest
    {
        uint32_t topic;
        uint32_t offset;
    };

    struct QueryResponseHeader
    {
        uint32_t resultCode;
        uint32_t payloadLength;
    };

public:
    explicit TcpQueryServer(unsigned int port, MsgBroker* broker);
    ~TcpQueryServer();

public:
    bool binding();
    void start();
    void stop();

private:
    void acceptLoop();

    /*
		1. 클라이언트로부터 요청을 받음 (recvAll로 고정된 크기의 데이터 수신 보장)
		2. 요청에서 토픽과 오프셋을 추출
		3. MsgBroker에서 해당 토픽과 오프셋에 대한 메시지를 가져옴
		4. 응답 헤더를 구성하여 클라이언트로 보냄 (sendAll로 고정된 크기의 데이터 송신 보장)
    */
    void clientWorker(SOCKET clientSocket);

private:
    /* 고정된 크기의 데이터를 다 받을 떄 까지 버퍼를 채움 */
    bool recvAll(SOCKET clientSocket, char* buffer, int length);
    /* 고정된 크기를 정확하게 다 보낼 때 까지 반복 */
    bool sendAll(SOCKET clientSocket, const char* buffer, int length);
};