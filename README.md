# Message Broker (개인 학습 프로젝트)

Kafka의 핵심 개념을 학습하기 위해 구현 중인 경량 메시지 브로커 프로젝트입니다.

## 구현 완료

### Topic 별 메시지 블록 분리

* Topic 단위로 메시지 저장 공간 분리
* `deque` 기반 블록 관리

### 메시지 저장 및 Offset 관리

* 메시지 분산 저장 구현
* Offset 증가 및 조회 기능 구현
* Offset 안정성 검증 완료

### Append-Only 저장 구조

* 메시지는 수정 없이 뒤에만 추가
* 블록 단위로 메시지 관리

### 자동 복구 기능

* 활성 블록이 가득 찰 경우 새로운 블록 생성
* 메시지 저장 중 블록 전환 및 복구 검증 완료

### UDP Receiver

* UDP 수신 기능 구현 완료
* 코드 정리 및 리팩토링 진행 중

---

## 개발 예정 기능

### TCP Sender

* Consumer 대상 메시지 전송 기능 구현

### 데이터 만료 정책

* TTL 기반 데이터 삭제
* 블록 단위 정리 정책 검토 중
* Reader 보호 방식(shared_ptr, Epoch, RCU 등) 검토 예정

---

## 성능 개선 TODO

### 1. 병목 구간 분석

현재 확인된 병목:

* `pushMessage()` 내부 Lock

개선 결과:

* Lock 범위 조정 시 약 25% 성능 향상 확인

### 2. Batch Append 지원

현재:

* 수신부는 Batch 처리 지원
* 저장부(`pushMessage`)는 단건 처리만 지원

개선 목표:

* `pushBatchMessage()` 구현
* Lock 횟수 감소
* Offset 갱신 횟수 감소
* 블록 상태 검사 횟수 감소

### 3. Lock-Free Read 구조 검토

현재:

* Append-Only 구조

검토 사항:

* Reader Lock 제거
* Published Offset 기반 조회
* Sealed Block 읽기 최적화

### 4. Write Lock 단순화

현재:

* Shared Lock 기반 구조

검토 사항:

* 일반 `mutex` 기반 단순화
* Reader Lock-Free 전환과 병행 적용

---

## 학습 목표

* Kafka의 Append-Only Log 구조 이해
* Offset 기반 메시지 관리 학습
* Batch Processing 최적화 실험
* Lock-Free Read 구조 실험
* Segment/Block 기반 저장 구조 연구
* 고성능 메시지 브로커 설계 경험 확보
