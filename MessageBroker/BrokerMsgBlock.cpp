#include "BrokerMsgBlock.h"
#include "ProducerReceiver.h"

BrokerMsgBlock::BrokerMsgBlock(unsigned int start_offset, int current_time, unsigned int max_ref_count, unsigned int max_buffer_size, unsigned int time_to_live_sec)
    :
    max_buf_size(max_buffer_size), /* 기본값으로 놔뒀다면 블록 당 8MB */
    max_ref_count(max_ref_count),  /* 최대 참조 -> 블록이 꽉차고 back 데이터 참조가 max_ref_count 만큼 일어났다면 삭제 가능 */
    time_to_live_sec(time_to_live_sec),
    created_time(current_time),
    start_msg_offset(start_offset)
{
#ifndef MEM_METHOD_COPY
    storage.reserve(max_buf_size); // 사용할 메모리 사전 점유
#else
    storage.resize(max_buf_size); // 사용할 메모리 사전 점유
#endif
}

bool BrokerMsgBlock::isDeleteAble(int cur_call_time)
{
    std::shared_lock<std::shared_mutex> lock(smtx);

    // 1. TTL(만료 시간) 초과 시 무조건 삭제 가능
    if (cur_call_time != -1 && (cur_call_time - created_time >= (int)time_to_live_sec)) {
        return true;
    }

    // 2. 버퍼가 꽉 찼거나 더 이상 데이터를 받지 않는 상태(state == false)이고,
    //    마지막 메시지(인덱스의 끝)까지 소비자가 약속된 횟수만큼 다 읽어갔다면 삭제 가능
    if (!write_state || cur_buf_size >= max_buf_size) {
        if (last_ref_cnt.load(std::memory_order_relaxed) >= max_ref_count) {
            return true;
        }
    }

    return false;
}

bool BrokerMsgBlock::push(const unsigned char* data, size_t length)
{
	unsigned int dataSize = static_cast<unsigned int>(length);

    std::lock_guard<std::shared_mutex> lock(smtx);
    if (write_state == false) return false;
    if (cur_buf_size + dataSize > max_buf_size)
    {
        write_state = false;
        return false; /* out-of-range */
    }

    insert(data, dataSize);

    return true;
}

bool BrokerMsgBlock::push(const std::vector<unsigned char>& data)
{
    /* 락 걸기 전에 미리 계산 */
    unsigned int dataSize = static_cast<unsigned int>(data.size());

    std::lock_guard<std::shared_mutex> lock(smtx);
    if (write_state == false) return false;
    if (cur_buf_size + dataSize > max_buf_size)
    {
        write_state = false;
        return false; /* out-of-range */
    }

    insert(data, dataSize);

    return true;
}

bool BrokerMsgBlock::refData(unsigned int idx, std::vector<unsigned char>& out_buf)
{
    std::shared_lock<std::shared_mutex> lock(smtx);
    unsigned int storageSize = static_cast<unsigned int>(storage_idx.size());
    if (storageSize <= idx) return false; /* out-of-range */
    if (!write_state && (idx == storageSize - 1))
    {
        last_ref_cnt.fetch_add(1, std::memory_order_relaxed);
    }

    ref(idx, out_buf);

    return true;
}

unsigned int BrokerMsgBlock::pushBatch(const std::vector<char>& data, const unsigned int used_buf_size, unsigned int& offset)
{
    static_assert(sizeof(ProducerMessage::MessageHeader) == 6, "Header Size check");
    
    std::lock_guard<std::shared_mutex> lock(smtx);

    if (write_state == false) return 0;
    const size_t headerSize = sizeof(ProducerMessage::MessageHeader);
	unsigned int processedSize = 0;
    

    while (offset + headerSize <= used_buf_size) /* 공간을 확인하면서 공간이 빌 때까지 진행 */
    {
        ProducerMessage::MessageHeader header; /* 배치 프로세스는 헤더 처리 안하고 바로 넘겨주기 때문에 헤더 때고 넣어줘야 함  */
        std::memcpy(&header, data.data() + offset, headerSize); /* 헤더 파싱 */
        unsigned int messageTotalSize = sizeof(header.length) + header.length;

		if (offset + messageTotalSize > used_buf_size) /* 메시지 사이즈가 남은 버퍼보다 큰 경우 -> 메시지가 완전히 들어오지 않은 경우이므로 처리 중단 */
        {
            break;
        }

        if (cur_buf_size + messageTotalSize > max_buf_size) /* 더이상 버퍼를 채울 수 없는 경우 */
        {
            write_state = false;
            break;
        }

        unsigned int payloadOffset = offset + sizeof(header.length); /* length 필드 다음부터 payload 시작 */
        unsigned int payloadLength = messageTotalSize - sizeof(header.length); /* length 필드 제외한 나머지 사이즈가 payload 사이즈 -> mark, topic, payload */

        insert(reinterpret_cast<const unsigned char*>(data.data() + payloadOffset), payloadLength);
        processedSize++;
        offset += messageTotalSize;
    }

	return processedSize; /* 처리한 버퍼 사이즈 반환 */
}


