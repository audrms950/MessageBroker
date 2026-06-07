# MessageBroker

고성능 메시지 수신 및 저장을 목표로 직접 구현한 C++ 기반 Message Broker 프로젝트.

단순한 메시지 큐 구현이 아니라 다음 항목을 학습하고 검증하기 위해 개발하였다.

- UDP 기반 고속 수신
- Batch Processing
- Append-Only Storage
- Offset 기반 조회
- Topic 분리 저장 구조
- Segment(Block) 관리
- 동시성 제어
- Memory Locality 최적화
- 성능 측정 기반 개선

---

# Architecture

```text
                 +------------------+
                 | UDP Producer     |
                 +--------+---------+
                          |
                          v
                 +------------------+
                 | ProducerReceiver |
                 | (UDP Receiver)   |
                 +--------+---------+
                          |
                     Batch Buffer
                          |
                          v
                 +------------------+
                 | MsgBroker        |
                 | Storage Engine   |
                 +--------+---------+
                          |
                    Topic Storage
                          |
                          v
                 +------------------+
                 | BrokerMsgBlock   |
                 | Segment Storage  |
                 +--------+---------+
                          |
                          v
                 +------------------+
                 | TCP Query Server |
                 +------------------+
```

---

# Project Goals

이 프로젝트는 단순히 메시지를 저장하는 프로그램이 아니라 다음 항목을 직접 구현하고 검증하기 위해 시작되었다.

- Producer / Consumer 구조 설계
- Batch Processing
- Buffer Pool 관리
- 고성능 메시지 수신
- Offset 기반 조회
- Segment 기반 저장 구조
- 동시성 제어
- 성능 측정 기반 최적화

---

# Why Batch Processing?

초기 구현은 패킷을 수신할 때마다 즉시 브로커에 전달하는 구조였다.

```text
recv
 -> parse
 -> insert
 -> recv
 -> parse
 -> insert
```

이 방식은

- 함수 호출 증가
- 락 획득 증가
- 캐시 효율 저하
- 메시지 단위 처리 오버헤드

문제가 발생했다.

현재는 일정량의 메시지를 수신 버퍼에 누적한 후 한 번에 파싱 및 저장한다.

```text
recv
recv
recv
recv
recv

↓

batch parse

↓

batch insert
```

이를 통해 처리량을 크게 향상시킬 수 있었다.

---

# Storage Design

## Append-Only Storage

메시지는 수정하지 않는다.

```text
Block A
[Msg][Msg][Msg][Msg]

Block B
[Msg][Msg][Msg][Msg]
```

Block이 가득 차면 자동으로 새로운 Block을 생성한다.

기존 Block은 읽기 전용 상태가 된다.

---

## Topic Based Storage

```text
Topic 1
 ├─ Block 1
 ├─ Block 2
 └─ Block 3

Topic 2
 ├─ Block 1
 └─ Block 2
```

토픽별 저장소를 분리하여 확장성과 락 경합 감소를 목표로 설계하였다.

---

## Offset Based Lookup

메시지는 Offset 기반으로 조회한다.

```text
Offset 0
Offset 1
Offset 2
Offset 3
Offset 4
```

조회 과정

1. Topic 선택
2. Binary Search로 Block 탐색
3. Block 내부 Offset 계산
4. Message 반환

---

# UDP Receiver Design

UDP 수신과 브로커 저장을 분리하였다.

## Buffer Pool

```text
Ready Queue
     ↓
 Receive Thread
     ↓
Process Queue
     ↓
 Worker Thread
     ↓
Ready Queue
```

버퍼를 재사용하여

- 동적 메모리 할당 감소
- 수신 지연 최소화
- 순간 트래픽 대응력 향상

을 목표로 하였다.

---

## Batch Receive

수신 스레드는 메시지를 즉시 브로커에 전달하지 않는다.

다음 조건 중 하나를 만족하면 Flush 한다.

- 일정 개수 이상 수신
- 버퍼 공간 부족
- Timeout 발생

---

# Broker Engine

Broker는 Topic별 Storage를 관리한다.

주요 특징

- Topic Storage 자동 생성
- Segment 자동 확장
- Binary Search 조회
- Offset Validation
- State Machine 기반 자가 복구
- TTL 기반 GC

---

# TCP Query Server

저장된 메시지를 조회하기 위한 TCP 서버를 구현하였다.

지원 기능

- Topic 조회
- Offset 조회
- Result Code 반환
- Payload 반환

```text
Client
   |
Query(topic, offset)
   |
   v
TCP Query Server
   |
   v
MsgBroker
   |
   v
Message Response
```

---

# Engine Performance Benchmark

## Test Case #1

| Item | Value |
|--------|--------|
| Message Count | 100,000 |
| Payload Size | 1,024 bytes |
| Elapsed Time | 0.113 sec |
| Throughput | 867 MB/s |
| Message Rate | 884,940 msg/s |

---

## Test Case #2

| Item | Value |
|--------|--------|
| Message Count | 100,000 |
| Payload Size | 1,000 bytes |
| Batch Count | 500 |
| Elapsed Time | 0.034 sec |
| Throughput | 2,810 MB/s |
| Message Rate | 2,935,690 msg/s |

---

## Test Case #3

| Item | Value |
|--------|--------|
| Message Count | 1,000,000 |
| Payload Size | 1,024 bytes |
| Batch Count | 500 |
| Elapsed Time | 0.420 sec |
| Throughput | 2,331 MB/s |
| Message Rate | 2,377,940 msg/s |

---

# End-to-End Benchmark

## UDP → Broker → TCP Query

### Test Condition

| Item | Value |
|--------|--------|
| Message Count | 1,000,000 |
| Payload Size | 1,024 bytes |
| Topic | 1 |
| UDP Port | 9000 |
| TCP Port | 9100 |

### Result

| Item | Result |
|--------|--------:|
| UDP Send Success | 1,000,000 |
| UDP Send Fail | 0 |
| UDP Send Time | 10.923 sec |
| UDP Send Rate | 91,546 msg/s |
| Stored Count | 999,997 |
| Lost Count | 3 |
| TCP Query Success | 999,997 |
| TCP Query Fail | 0 |
| TCP Query Time | 55.242 sec |
| TCP Query Rate | 18,101 msg/s |
| Total Time | 76.186 sec |

### Analysis

```text
Expected : 1000000
Stored   : 999997
Lost     : 3

Receiver Packet Count : 999997
```

브로커 저장 수와 Receiver 수신 수가 동일하였다.

따라서 현재 테스트 기준

- Broker 내부 저장 유실 없음
- UDP 수신 단계에서만 3건 유실

로 판단된다.

또한 저장된 메시지에 대해 TCP Query 검증 결과

```text
Query Success : 999997
Query Fail    : 0
```

조회 정합성 또한 확인하였다.

---

# Current Bottleneck

현재 가장 큰 병목은 TCP Query 경로이다.

조회 시 다음 비용이 포함된다.

```cpp
out_buf.assign(
    storage.begin() + curIdx.start_offset,
    storage.begin() + curIdx.start_offset + curIdx.length);
```

포함 비용

- Binary Search
- Offset 계산
- Vector Resize
- Memory Allocation
- Message Copy

현재 TCP 조회 속도는 UDP 쓰기 속도 대비 약 5배 정도 느리다.

---

# Future Work

## Read Path Optimization

- Zero Copy 조회
- Ref 기반 API
- Direct Payload Reference
- Lock-Free Read 검토

예상 구조

```cpp
struct RefData
{
    const unsigned char* data;
    unsigned int length;
};
```

---

## Network Layer

- Multi Receiver
- Multi Port Receiver
- Receiver Auto Scaling
- TCP Batch Response

---

## Storage Layer

- Segment TTL
- Snapshot
- Disk Persistence
- Recovery Mechanism

---

# Lessons Learned

이 프로젝트를 통해 다음 내용을 직접 구현하며 검증하였다.

- Batch Processing
- Buffer Pool
- Producer / Consumer
- Shared Mutex
- Topic Partitioning
- Append-Only Storage
- Segment Rollover
- Binary Search Lookup
- Offset 기반 조회
- UDP 고속 수신
- TCP Query Server
- 성능 측정 기반 최적화

단순히 동작하는 브로커를 만드는 것이 아니라,

**"병목을 측정하고, 원인을 분석하고, 구조를 개선하는 과정"**

에 집중한 프로젝트이다.