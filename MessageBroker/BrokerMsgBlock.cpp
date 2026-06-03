#include "BrokerMsgBlock.h"

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
