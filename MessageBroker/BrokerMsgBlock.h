#pragma once
#include <mutex>
#include <memory>
#include <vector>
#include <shared_mutex>
#include <atomic>
#include <algorithm>


/* x86-64 아키텍처는 64  x86 은 32 */
#define CACHE_BLOCK 64
/* 끄는 방식이 더 안전하긴 함 근데 켜도 문제 안생기게 구성해서 기본은 켜둠 */
#define MEM_METHOD_COPY

class BrokerMsgBlock
{
private:
    struct MsgIndex {
        unsigned int start_offset;   // 통버퍼 내에서의 시작 바이트 위치
        size_t length;        // 패킷의 순수 바이트 길이
    };

private:
    std::shared_mutex smtx; /* 구성한 쓰기, 읽기 쓰레드 비율이 1 : n 이므로 shared_mutex 로 읽기를 병렬처리 하는게 좋음  */
    std::vector<unsigned char> storage;
    std::vector<MsgIndex>storage_idx; /* 데이터를 가져갈 인덱스 */
    bool write_state = true;


private: /* 자주 변경되는 데이터들은 캐시 블록 크기에 맞춰 패딩되도록 구성 */
    alignas(CACHE_BLOCK) unsigned int cur_data_cnt = 0;
    alignas(CACHE_BLOCK) unsigned int cur_buf_size = 0;
    std::atomic<unsigned int> last_ref_cnt = 0;

public: /* 환경 상수*/
    const size_t max_buf_size;
    const unsigned int max_ref_count;
    const unsigned int time_to_live_sec;
    const int created_time; /* 블록 생성 시간 기록용 */
    const unsigned int start_msg_offset;
public:
    BrokerMsgBlock(unsigned int start_offset, int current_time, unsigned int max_ref_count = 1, unsigned int max_buffer_size = 1024 * 8192, unsigned int time_to_live_sec = 3600 * 24 * 31);

    bool isDeleteAble(int cur_call_time);
	bool push(const unsigned char* data, size_t length);
    bool push(const std::vector<unsigned char>& data);
    bool refData(unsigned int idx, std::vector<unsigned char>& out_buf);

    /* 처리한 메시지 개수 반환 */
    unsigned int pushBatch(const std::vector<char>& data, const unsigned int used_buf_size, unsigned int& offset);
public:
    inline bool isFull()
    {
        std::shared_lock<std::shared_mutex> lock(smtx);
        return !write_state;
    }

    inline unsigned int getLastOffset()
    {
        std::shared_lock<std::shared_mutex> lock(smtx);
        return  start_msg_offset + cur_data_cnt;
    }

private: /* 프리미티브 -> NO (Check, lock) 위의 호출자에서 다 보장하고 오는 것을 전제로 구성  */
	

    inline void insert(const unsigned char* data, const unsigned int data_size)
    {
#ifndef MEM_METHOD_COPY
        storage.insert(storage.end(), data.begin(), data.end());
#else
        //std::copy(data.begin(), data.end(), storage.begin() + cur_buf_size);
        std::memcpy(&storage[cur_buf_size], data, data_size);
#endif
        storage_idx.emplace_back(cur_buf_size, data_size);
        cur_data_cnt++;
        cur_buf_size += data_size;
    }
    
    inline void insert(const std::vector<unsigned char>& data, const unsigned int data_size)
    {
#ifndef MEM_METHOD_COPY
        storage.insert(storage.end(), data.begin(), data.end());
#else
        //std::copy(data.begin(), data.end(), storage.begin() + cur_buf_size);
        std::memcpy(&storage[cur_buf_size], data.data(), data_size);
#endif
        storage_idx.emplace_back(cur_buf_size, data_size);
        cur_data_cnt++;
        cur_buf_size += data_size;
    }

    inline void ref(unsigned int idx, std::vector<unsigned char>& out_buf)
    {
        const MsgIndex& curIdx = storage_idx[idx];
        out_buf.assign(
            storage.begin() + curIdx.start_offset,
            storage.begin() + curIdx.start_offset + curIdx.length);
    }
};