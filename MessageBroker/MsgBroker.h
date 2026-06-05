#pragma once
#include "BrokerMsgBlock.h"
#include <unordered_map>
#include <deque>
#include <functional>
#include <vector>
#include <shared_mutex>
#include <thread>

class MsgBroker
{
private:
	static constexpr int GC_INTERVAL_MS = 1000 * 5;
	static constexpr int LOCK_COUNT = 64;

public:
	enum state
	{
		fail,
		ok,
		create_storage,
		insert_data
	};

	enum error
	{
		batch_buf_out_of_bound = -1, /* 배치 버퍼 내부의 패킷이 블록보다 큼 */
		batch_buf_size_min = -2, /* 배치 버퍼가 헤더보다 작음 */

		batch_buf_error = -3,
	};

	enum code
	{
		code_fail = 0,
		code_ok,

		out_of_bound,
		out_of_range

	};

private:
	/* map<topic/storage> */
	std::unordered_map<int, std::deque<std::unique_ptr<BrokerMsgBlock>>> deque_storage;
	/* container smtx */
	std::shared_mutex mtx_storage_lock; /* 이게 막히면 lock 획득 자체가 막히므로 전체 프로세스가 멈춤 */
	/* topic smtx -> 생성자에서 사전 할당 후 토픽 별로 분배하여 사용 */
	std::vector<std::unique_ptr<std::shared_mutex>>topic_locks;
	/* lock 분배용 hasher */
	std::hash<int> hasher;
	/* 브로커 쓰레드 */
	std::atomic<bool> gcRun = true;
	std::vector<std::thread>threads;/* 쓰레드 RAII 패턴으로 삭제할 땐 벡터를 맨 밑에 두는게 좋음 -> 위에서부터 삭제되기 때문 */
public:
	/* 성능검사용 -> TODO : 매크로로 테스트 분기처리 하는게 나을듯 */
	std::atomic<unsigned int> totalStoredMessageCount = 0;
	unsigned int getStoredMessageCount() const
	{
		return totalStoredMessageCount.load(std::memory_order_relaxed);
	}

	MsgBroker();
	~MsgBroker();
	void pushMessage(int topic, const unsigned char* data, size_t length);
	void pushMessage(int topic, const std::vector<unsigned char>& data);
	void pushBatch(int topic, const std::vector<char>& data, const unsigned int used_buf_size);
	code getMessage(int topic, unsigned int offset, std::vector<unsigned char>& out_buf);

private:
	/* 토픽 garbege collector */
	void topicGarbageCollector();

	/* 토빅 별 가드 생성 */
	inline std::shared_mutex* getTopicMtx(int topic)
	{
		unsigned int lockIndx = hasher(topic) & (LOCK_COUNT - 1);

		return topic_locks[lockIndx].get();
	}

private: /* 프리미티브 -> NO lock 위의 호출자에서 다 보장하고 오는 것을 전제로 구성  */
	
	void errorCodeHandling(int errorcode);


	bool onCreateStorage(int topic);
	bool onDeleteStorage(int topic);

	/* state muchine 패턴으로 실패 시 자가복구 하고 삽입까지 하도록 구성
	*	-> 나와서 분기처리하고 다시 시작하면 낭비 너무 심함 스택 옮기는거 없이 혼자 빠르게 처리하도록 구현
	*/
	void insert(int topic, const unsigned char* data, size_t length);
	void insert(int topic, const std::vector<unsigned char>& data);
	/*
		lower_bound 기반 이진 검색으로 해당 토픽의 블록들 중에서 offset이 속하는 블록을 빠르게 찾아서 반환
	*/
	BrokerMsgBlock* findBlock(int topic, unsigned int offset);

	/* 
		topic과 offset기반으로 블록을 찾고(findBlock) 해당 블록에서 offset에 해당하는 데이터를 참조(refData)하여 out_buf에 복사
	*/
	code ref(int topic, unsigned int offset, std::vector<unsigned char>& out_buf);

	long long getCurTimeSec();
};