#include "MsgBroker.h"

MsgBroker::MsgBroker()
{
	topic_locks.reserve(BrokerConst::LOCK_COUNT);
	for (int i = 0; i < BrokerConst::LOCK_COUNT; ++i)
	{
		topic_locks.push_back(std::make_unique<std::shared_mutex>());
	}
}

void MsgBroker::pushMessage(int topic, const unsigned char* data, size_t length)
{
	// 1. 항상 큰 범위의 storage 락을 '공유 락'으로 먼저 체크 (데드락 방지 및 상시 병목 제거)
	std::shared_lock<std::shared_mutex> mgr_shared_lock(mtx_storage_lock);

	if (deque_storage.find(topic) == deque_storage.end()) {
		// 토픽이 없을 때만 공유 락을 일시 해제하고 '독점 락'으로 전환하여 생성
		mgr_shared_lock.unlock();

		std::unique_lock<std::shared_mutex> mgr_unique_lock(mtx_storage_lock);
		if (deque_storage.find(topic) == deque_storage.end()) {
			onCreateStorage(topic);
		}
	}
	else {
		// 이미 토픽이 존재한다면 조회용 공유 락을 명시적으로 해제
		mgr_shared_lock.unlock();
	}

	// 2. 이제 맵 구조 변경 우려가 없으므로 해당 토픽의 전용 분할 락만 잡고 삽입 수행
	auto* topicMtx = getTopicMtx(topic);
	std::unique_lock<std::shared_mutex> write_lock(*topicMtx);

	insert(topic, data, length);
}

void MsgBroker::pushMessage(int topic, const std::vector<unsigned char>& data)
{
	// 1. 항상 큰 범위의 storage 락을 '공유 락'으로 먼저 체크 (데드락 방지 및 상시 병목 제거)
	std::shared_lock<std::shared_mutex> mgr_shared_lock(mtx_storage_lock);

	if (deque_storage.find(topic) == deque_storage.end()) {
		// 토픽이 없을 때만 공유 락을 일시 해제하고 '독점 락'으로 전환하여 생성
		mgr_shared_lock.unlock();

		std::unique_lock<std::shared_mutex> mgr_unique_lock(mtx_storage_lock);
		if (deque_storage.find(topic) == deque_storage.end()) {
			onCreateStorage(topic);
		}
	}
	else {
		// 이미 토픽이 존재한다면 조회용 공유 락을 명시적으로 해제
		mgr_shared_lock.unlock();
	}

	// 2. 이제 맵 구조 변경 우려가 없으므로 해당 토픽의 전용 분할 락만 잡고 삽입 수행
	auto* topicMtx = getTopicMtx(topic);
	std::unique_lock<std::shared_mutex> write_lock(*topicMtx);
	insert(topic, data);
}

void MsgBroker::pushBatch(int topic, const std::vector<char>& data, const unsigned int used_buf_size)
{
	std::shared_lock<std::shared_mutex> mgr_shared_lock(mtx_storage_lock);

	if (deque_storage.find(topic) == deque_storage.end()) {
		// 토픽이 없을 때만 공유 락을 일시 해제하고 '독점 락'으로 전환하여 생성
		mgr_shared_lock.unlock();

		std::unique_lock<std::shared_mutex> mgr_unique_lock(mtx_storage_lock);
		if (deque_storage.find(topic) == deque_storage.end()) {
			onCreateStorage(topic);
		}
	}
	else {
		// 이미 토픽이 존재한다면 조회용 공유 락을 명시적으로 해제
		mgr_shared_lock.unlock();
	}

	// 2. 이제 맵 구조 변경 우려가 없으므로 해당 토픽의 전용 분할 락만 잡고 삽입 수행
	auto* topicMtx = getTopicMtx(topic);
	std::unique_lock<std::shared_mutex> write_lock(*topicMtx); 

	auto& dq = deque_storage[topic];
	unsigned int useBuffer = 0;

	while (useBuffer < used_buf_size)
	{
		/* 꽉차면 새 블록 생성해서 이어서 쓰기 */
		if (dq.back()->isFull() == true)
		{
			onCreateStorage(topic);
		}
		else
		{
			int processedMsgCnt = dq.back()->pushBatch(data, used_buf_size, useBuffer);
			totalStoredMessageCount.fetch_add(processedMsgCnt, std::memory_order_relaxed);
		}
	}
}

MsgBroker::code MsgBroker::getMessage(int topic, unsigned int offset, std::vector<unsigned char>& out_buf)
{
	auto* topicMtx = getTopicMtx(topic);
	if (topicMtx == nullptr) return out_of_bound;

	// 1. 읽을 때도 맵 조회를 위해 큰 락을 먼저 거치되, 검사 후 '즉시' 해제하여 병목 제거
	std::shared_lock<std::shared_mutex> mgr_shared_lock(mtx_storage_lock);
	if (deque_storage.find(topic) == deque_storage.end()) {
		return out_of_bound;
	}
	mgr_shared_lock.unlock(); // 검사 끝났으니 바로 풀어줌으로써 다른 스레드의 진입을 방해하지 않음

	// 2. 이제 해당 토픽의 분할 락만 공유 락으로 잡고 안전하게 데이터를 읽음
	std::shared_lock<std::shared_mutex> read_lock(*topicMtx);
	return ref(topic, offset, out_buf);
}

bool MsgBroker::onCreateStorage(int topic)
{
	int curTime = 0;/* TODO : 현재 미구현 상태 */
	unsigned int nextOffset = 0;
	auto it = deque_storage.find(topic);
	if (it != deque_storage.end())
	{
		auto& lastMsgBlock = it->second.back();
		if (lastMsgBlock->isFull() == false)
		{
			return false;
		}
		else
		{
			nextOffset = lastMsgBlock->getLastOffset();
		}
	}

	deque_storage[topic].push_back(std::make_unique<BrokerMsgBlock>(nextOffset, curTime));

	return true;
}

void MsgBroker::insert(int topic, const unsigned char* data, size_t length)
{
	state curState = insert_data;
	auto it = deque_storage.find(topic);
	if (it == deque_storage.end()) curState = create_storage;

	/* TODO : 나중에 실패 케이스 추가되면 fail 도 할당하도록 */
	do /* 최초 한 번은 무조건 실행되므로 do-while 사용 */
	{
		switch (curState)
		{
		case insert_data:
		{
			curState = it->second.back()->push(data, length) == true ? ok : create_storage;
			totalStoredMessageCount.fetch_add(1, std::memory_order_relaxed);
			break;
		}
		case create_storage:
			onCreateStorage(topic); /* 스토리지 세그먼트 추가 */
			it = deque_storage.find(topic);
			curState = insert_data;
			break;
		}

	} while (curState != ok && curState != fail);
}

void MsgBroker::insert(int topic, const std::vector<unsigned char>& data)
{
	state curState = insert_data;
	auto it = deque_storage.find(topic);
	if (it == deque_storage.end()) curState = create_storage;

	/* TODO : 나중에 실패 케이스 추가되면 fail 도 할당하도록 */
	do /* 최초 한 번은 무조건 실행되므로 do-while 사용 */
	{
		switch (curState)
		{
		case insert_data:
		{
			curState = it->second.back()->push(data) == true ? ok : create_storage;
			totalStoredMessageCount.fetch_add(1, std::memory_order_relaxed);
			break;
		}
		case create_storage:
			onCreateStorage(topic); /* 스토리지 세그먼트 추가 */
			it = deque_storage.find(topic);
			curState = insert_data;
			break;
		}

	} while (curState != ok && curState != fail);

}

BrokerMsgBlock* MsgBroker::findBlock(int topic, unsigned int offset)
{
	auto storageIt = deque_storage.find(topic);
	if (storageIt == deque_storage.end() || storageIt->second.empty()) return nullptr;

	auto& blocks = storageIt->second;

	/* 이게 logN 시간복잡도라 금방 찾음 */
	auto it = std::lower_bound(blocks.begin(), blocks.end(), offset,
		[](const std::unique_ptr<BrokerMsgBlock>& block, unsigned int val)
		{
			return block->getLastOffset() <= val; // 경계값 기준 판단
		});

	if (it != blocks.end() && offset >= (*it)->start_msg_offset && offset < (*it)->getLastOffset()) {
		return it->get();
	}

	return nullptr; // 이미 지워졌거나 아직 생성되지 않은 미래의 오프셋인 경우
}

MsgBroker::code MsgBroker::ref(int topic, unsigned int offset, std::vector<unsigned char>& out_buf)
{
	BrokerMsgBlock* block = findBlock(topic, offset);
	if (block == nullptr) return out_of_bound;

	unsigned int blockOffset = offset - block->start_msg_offset;

	return block->refData(blockOffset, out_buf) == true ? code_ok : out_of_range;
}
