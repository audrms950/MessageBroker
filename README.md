# Performance Benchmark

## Test Environment

### Broker Architecture

* Append-Only Storage
* Topic 별 Block(Segment) 분리
* Offset 기반 메시지 조회
* Batch Insert 지원
* Block Full 시 자동 Rollover
* Binary Search 기반 Block 탐색
* Memory Only Benchmark (Disk I/O 제외)

---

## Write Performance

### Test Case #1

| Item          | Value         |
| ------------- | ------------- |
| Message Count | 100,000       |
| Payload Size  | 1,024 bytes   |
| Packet Size   | 1,028 bytes   |
| Batch Count   | 1             |
| Total Size    | 98.03 MB      |
| Elapsed Time  | 0.113 sec     |
| Throughput    | 867 MB/s      |
| Message Rate  | 884,940 msg/s |

### Test Case #2

| Item          | Value           |
| ------------- | --------------- |
| Message Count | 100,000         |
| Payload Size  | 1,000 bytes     |
| Packet Size   | 1,004 bytes     |
| Batch Count   | 500             |
| Total Size    | 95.74 MB        |
| Elapsed Time  | 0.034 sec       |
| Throughput    | 2,810 MB/s      |
| Message Rate  | 2,935,690 msg/s |

### Test Case #3

| Item          | Value           |
| ------------- | --------------- |
| Message Count | 1,000,000       |
| Payload Size  | 1,024 bytes     |
| Packet Size   | 1,028 bytes     |
| Batch Count   | 500             |
| Total Size    | 980.37 MB       |
| Elapsed Time  | 0.420 sec       |
| Throughput    | 2,331 MB/s      |
| Message Rate  | 2,377,940 msg/s |

---

## Batch Performance Analysis

Batch Insert를 적용하지 않은 경우 약 867 MB/s 수준의 처리량을 기록하였다.

Batch Insert 적용 시 약 2.3 ~ 2.8 GB/s 수준까지 처리량이 향상되었으며, 테스트 결과 Batch Count 500 ~ 1000 구간에서 가장 높은 처리량을 보였다.

실험 결과 Batch 크기를 무한정 증가시키는 것이 성능 향상으로 이어지지 않았으며, 일정 크기 이상에서는 오히려 처리량이 감소하였다.

이는 CPU Cache Locality, Memory Bandwidth, Block Rollover 비용 등의 영향으로 판단된다.

---

## Read Performance

### Test Case

| Item               | Value         |
| ------------------ | ------------- |
| Message Count      | 1,000,000     |
| Payload Size       | 1,024 bytes   |
| Read Thread Count  | 8             |
| Read Success Count | 1,000,000     |
| Read Fail Count    | 0             |
| Elapsed Time       | 5.54 sec      |
| Message Rate       | 180,494 msg/s |

---

## Read Benchmark Limitation

현재 Read Benchmark는 실제 메시지를 반환하기 위해 아래와 같은 복사 작업을 수행한다.

```cpp
out_buf.assign(
    storage.begin() + curIdx.start_offset,
    storage.begin() + curIdx.start_offset + curIdx.length);
```

따라서 현재 측정값은 순수 조회 성능이 아닌 다음 비용이 포함된 결과이다.

* Block 탐색
* Offset 조회
* Vector Resize
* Memory Allocation
* Message Copy (memcpy)

테스트 기준 약 1GB 이상의 데이터를 실제로 복사하고 있으므로, 현재 Read 성능 수치는 Storage Engine 자체의 조회 성능보다 보수적으로 측정된 결과이다.

---

## Planned Optimization

### Read Path

* Shared Lock 제거 완료
* Lock-Free Read 적용 예정
* Ref 기반 조회 API 추가 예정
* Zero-Copy 방식 검토

예상 구조

```cpp
struct RefData
{
    const unsigned char* data;
    unsigned int length;
};
```

이를 통해 메시지 복사 없이 Storage를 직접 참조하도록 개선할 계획이다.

---

## Current Status

### Implemented

* Topic 기반 Storage 분리
* Append-Only Storage
* Block(Segment) Rollover
* Offset Index
* Batch Insert
* Binary Search 기반 Block 탐색
* Multi Thread Read Test
* Offset Validation
* Storage Recovery

### In Progress

* UDP Receiver
* TCP Sender
* TTL 기반 Segment Expire
* Lock-Free Read
* Batch Size Auto Tuning

---

## Summary

현재 구현된 Broker Engine은 Append-Only 기반 Memory Storage 구조를 사용하며, Batch Insert 적용 시 약 2.3 ~ 2.8 GB/s 수준의 처리량을 기록하였다.

100만 건 이상의 메시지 저장 및 조회 검증을 통과하였으며, Block Rollover 및 Offset 정합성 검증 또한 완료하였다.

향후 Lock-Free Read 및 Zero-Copy 기반 Ref API를 추가하여 Read 성능을 추가 개선할 예정이다.
