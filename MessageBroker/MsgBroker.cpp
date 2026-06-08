#include "MsgBroker.h"
#include <iostream>
#include <memory>


MsgBroker::MsgBroker()
{
	topic_locks.reserve(LOCK_COUNT);
	for (int i = 0; i < LOCK_COUNT; ++i)
	{
		topic_locks.push_back(std::make_unique<std::shared_mutex>());
	}

	std::thread gc_topic(&MsgBroker::topicGarbageCollector, this);
	threads.push_back(std::move(gc_topic));
}

MsgBroker::~MsgBroker()
{
	gcRun = false;

	for (auto& thread : threads)
	{
		if (thread.joinable())
		{
			thread.join();
		}
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
	unsigned int processContinueCnt = 0;

	/* 무한루프 방지 코드 추가 
		버퍼 소모를 안시키거나, 못시키는 상태가 반복된다면 현재 배치 버퍼를 폐기처리 (processContinueCnt)
	*/
	while (useBuffer < used_buf_size && processContinueCnt < 2)
	{
		/* 꽉차면 새 블록 생성해서 이어서 쓰기 */
		if (dq.back()->isFull() == true)
		{
			onCreateStorage(topic);
			processContinueCnt = 0;
		}
		else
		{
			int processedMsgCnt = dq.back()->pushBatch(data, used_buf_size, useBuffer);
			if (processedMsgCnt == 0) processContinueCnt++; /* 버퍼 소모를 안시키거나, 못시키는 상태 누적 */
			else if (processedMsgCnt < 0) /* 처리 개수가 아닌 에러코드 반환 */
			{ 
				errorCodeHandling(processedMsgCnt);
				useBuffer = used_buf_size; /* 전여 버퍼 강제 폐기 */
				break; /* 에러 발생 시 루프 종료하여 버퍼 폐기 */
			}
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
	if (deque_storage.find(topic) == deque_storage.end()) 
	{
		return out_of_bound;
	}
	mgr_shared_lock.unlock(); // 검사 끝났으니 바로 풀어줌으로써 다른 스레드의 진입을 방해하지 않음

	// 2. 이제 해당 토픽의 분할 락만 공유 락으로 잡고 안전하게 데이터를 읽음
	std::shared_lock<std::shared_mutex> read_lock(*topicMtx);
	return ref(topic, offset, out_buf);
}

int MsgBroker::getMessage(int topic, unsigned int offset, 
	const char*& outBuf, const BrokerMsgBlock::MsgIndex*& bufIdx)
{
	outBuf = nullptr;
	bufIdx = nullptr;

	auto* topicMtx = getTopicMtx(topic);
	if (topicMtx == nullptr)
	{
		return out_of_bound;
	}

	std::shared_lock<std::shared_mutex> mgrSharedLock(mtx_storage_lock);

	if (deque_storage.find(topic) == deque_storage.end())
	{
		return out_of_bound;
	}

	mgrSharedLock.unlock();

	std::shared_lock<std::shared_mutex> readLock(*topicMtx);

	BrokerMsgBlock* block = findBlock(topic, offset);
	if (block == nullptr)
	{
		return out_of_bound;
	}

	unsigned int blockOffset = offset - block->start_msg_offset;

	if (block->getMsgCount() <= blockOffset)
	{
		return out_of_idx;
	}

	outBuf = reinterpret_cast<const char*>(block->getStoragePtr());
	bufIdx = block->getMsgIndex(blockOffset);

	return code_ok;
}

void MsgBroker::topicGarbageCollector()
{
	while(gcRun.load())
	{
		long long cur = getCurTimeSec();
		std::vector<int> emptyTopic;  /* 삭제 예정 스토리지 */
		{
			std::shared_lock<std::shared_mutex> storage_shared_lock(mtx_storage_lock); /* 읽기 전용 락 획득 */
			for (auto& topicStorage : deque_storage)
			{
				int topic = topicStorage.first;
				if (topicStorage.second.empty())
				{
					emptyTopic.push_back(topic);
					continue;
				}
				
				{
					auto* topicMtx = getTopicMtx(topic);
					std::shared_lock<std::shared_mutex> topic_shared_lock(*topicMtx); /* 읽기전용으로 먼저 검사 */
					
					/* NOTE : 상제 정책은 메시지 MessageBlock 안에만 있으므로 GC를 수정하지 않아도 되게끔 구성하여야 함 
						-> 정책 추가 시 블록안에다가 규칙을 넣고 isDeleteAble로 검사만 하게끔 해야 함  */
					if (topicStorage.second.front()->isDeleteAble(cur)) /* 삭제가 가능하다면 쓰기 락으로 전환 후 삭제 시작 */
					{
						topic_shared_lock.unlock();
						std::lock_guard<std::shared_mutex>write_lock(*topicMtx);
						/* 락을 다시 잡는동안 데이터가 쓰여질 수 있는데, onDeleteStorage 내부에서 다시 검사하므로 안전함
							->isDeleteAble(cur) 를 중복 호출하긴 하는데 안전성을 위해 놔둠 */
						onDeleteStorage(topic); /* 이거 수행 이후에 empty 될 수 있는데 다음 루프에 삭제됨 일부러 놔둔거임 */
					}
				}
			}
		}

		if (!emptyTopic.empty())
		{
			std::lock_guard<std::shared_mutex>write_lock(mtx_storage_lock); /* 삭제는 락 점유한 상태로 쭉 돌기 */
			for (int topic : emptyTopic)
			{
				auto it = deque_storage.find(topic);
				/* 락 점유가 풀렸다가 다시 시작되므로 안전검사 재수행 */
				if(it != deque_storage.end() && it->second.empty())
				{
					deque_storage.erase(topic);
				}
			}
		}

		/* GC 실행 시 공유락을 이용하고, 검사 비용이 싸서 인터벌 짧게 자주 검사하도록 함
			TODO : 규모가 커지고 인터벌이 늘어나면 종료 지연이 발생할 수 있으므로 그 경우에는 cv 블로킹 검토 
		*/
		std::this_thread::sleep_for(std::chrono::milliseconds(GC_INTERVAL_MS)); /* GC_INTERVAL_MS = 1000 * 5 */
	}
}

void MsgBroker::errorCodeHandling(int errorcode)
{
	if (errorcode >= 0) /* 에러 코드는 음수만 사용 */
	{
		std::cerr << "code " << errorcode << " is Not error code" << std::endl;
	}

	switch (errorcode) {
	case error::batch_buf_error: /* 버퍼 정합성 이슈 */
		std::cerr << " -> Reason: [batch_buf_error] Internal batch processing or logic error inside the block." << std::endl;
		break;

	case error::batch_buf_out_of_bound: /* 버퍼 크기가 전체 블록 크기보다 큼 */
		std::cerr << " -> Reason: [batch_buf_out_of_bound] Attempted to access memory outside the allocated buffer boundaries." << std::endl;
		break;

	case error::batch_buf_size_min: /* 버퍼가 헤더보다 같거나 작음 */
		std::cerr << " -> Reason: [batch_buf_size_min] Remaining data size is smaller than the minimum required message header size." << std::endl;
		break;

	default: /* 정의되지 않은 에러 */
		std::cerr << " -> Reason: Unknown error code encountered." << std::endl;
		break;
	}
}

bool MsgBroker::onCreateStorage(int topic)
{
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

	deque_storage[topic].push_back(std::make_unique<BrokerMsgBlock>(nextOffset, getCurTimeSec()));

	return true;
}

bool MsgBroker::onDeleteStorage(int topic)
{
	auto it = deque_storage.find(topic);
	if (it == deque_storage.end()) return false;
	else if (it->second.empty()) return false;
	
	long long curTIme = getCurTimeSec();
	auto& topicStorage = it->second;
	bool isDeletable = false;
	do /* 토픽은 앞에서만 삭제 TTL 및 참조 카운트 검사 */
	{
		isDeletable = topicStorage.front()->isDeleteAble(curTIme);
		if (isDeletable)
		{
			topicStorage.pop_front();
			isDeletable = !topicStorage.empty(); /* 토픽이 더 있다면 검사 시작 */
		}

	} while (isDeletable);
	
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

long long MsgBroker::getCurTimeSec()
{
	return std::chrono::duration_cast<std::chrono::seconds>(
		std::chrono::steady_clock::now().time_since_epoch()).count();
}
