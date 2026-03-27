# CDC 경로 A: Kafka Connect + Debezium

이 문서는 `feat/week7-kafka-cdc-connect` 브랜치 기준의 로컬 실행 절차를 설명한다.

## 역할 분담

- Connect: binlog 소비, 토픽 라우팅, 오프셋 관리
- Debezium MySQL Connector: MySQL row change -> Kafka Connect record 변환
- 애플리케이션: CDC 토픽 소비(필요 시), 멱등/집계 반영

## 기동 순서

1. Kafka 기동
   - `docker compose -f docker/infra-compose.yml up -d kafka`
2. CDC 전용 MySQL + Connect 기동
   - `docker compose -f docker/cdc/connect-compose.yml up -d`
3. 커넥터 등록
   - `./docker/cdc/register-connector.sh`

## 기본 커넥터 설정

- 파일: `docker/cdc/connectors/mysql-loopers-connector.json`
- 소스 DB: `loopers`
- 포함 테이블:
  - `loopers.product_metrics`
  - `loopers.outbox_event`
- 토픽 prefix: `loopers-cdc`
- 최종 라우팅 토픽: `cdc-connect-<table>`
  - 예: `cdc-connect-product_metrics`

## 확인

- Connect 상태:
  - `curl http://localhost:8083/connectors/loopers-mysql-cdc/status`
- 토픽 확인:
  - Kafka UI(`http://localhost:9099`)에서 `cdc-connect-*` 토픽 확인

## 주의

- Polling Outbox 경로와 동시에 같은 의미의 이벤트를 같은 토픽으로 보내면 중복 발행이 발생한다.
- CDC 실험 시에는 `cdc-connect-*` 등 별도 토픽을 유지한다.
