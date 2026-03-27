# Kafka Connect + Debezium CDC 실행 가이드

## 1) 인프라 기동

프로젝트 루트에서 실행:

```bash
docker compose -f docker/cdc/docker-compose.connect.yml up -d
```

확인:

```bash
curl http://localhost:8083/connectors
```

## 2) 커넥터 등록

```bash
curl -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  --data @docker/cdc/connectors/mysql-loopers-cdc.json
```

상태 확인:

```bash
curl http://localhost:8083/connectors/mysql-loopers-cdc/status
```

## 3) CDC 이벤트 확인

MySQL(`localhost:3307`)에서 `loopers` DB 테이블에 INSERT/UPDATE/DELETE를 발생시키면
Kafka 토픽(`loopers.<db>.<table>`)으로 이벤트가 발행된다.

예시 토픽 조회:

```bash
docker exec -it cdc-kafka kafka-topics.sh --bootstrap-server localhost:9092 --list
```

## 4) 정리

```bash
docker compose -f docker/cdc/docker-compose.connect.yml down -v
```
