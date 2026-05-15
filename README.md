# SOA HW №6
# Мозгин Н.С.

## Запуск

```bash
make docker-rebuild
```

## Линтеры

```bash
make lint
```

## Обоснование Cassandra data model

Схема находится в `cassandra/migrations/*.cql`.

### inventory_by_product_zone

```sql
PRIMARY KEY ((product_id), zone_id)
```

Эта таблица отвечает на запрос:

```sql
SELECT *
FROM inventory_by_product_zone
WHERE product_id = ? AND zone_id = ?;
```

`product_id` выбран partition key, потому что основной доступ начинается от SKU
товара. `zone_id` выбран clustering key, потому что внутри одного товара нужно
быстро получить строку конкретной зоны. Такая модель также позволяет получить
все зоны товара запросом по одному `product_id`.

### inventory_by_product

```sql
PRIMARY KEY ((product_id))
```

Эта таблица хранит агрегированный остаток товара по всем зонам:

```sql
SELECT *
FROM inventory_by_product
WHERE product_id = ?;
```

Здесь нужен только partition key `product_id`, потому что на один товар хранится
одна агрегированная строка: `total_available_quantity` и
`total_reserved_quantity`.

### inventory_by_zone

```sql
PRIMARY KEY ((zone_id), product_id)
```

Эта таблица отвечает на запрос:

```sql
SELECT *
FROM inventory_by_zone
WHERE zone_id = ?;
```

`zone_id` выбран partition key, потому что запрос начинается от зоны склада:
нужно получить все товары, лежащие в конкретной зоне. `product_id` выбран
clustering key, чтобы внутри partition зоны хранить строки по товарам.

### processed_events

```sql
PRIMARY KEY ((event_id))
```

Эта таблица нужна для идемпотентности. Consumer перед обработкой проверяет, был
ли уже обработан конкретный `event_id`:

```sql
SELECT event_id
FROM processed_events
WHERE event_id = ?;
```

`event_id` является partition key, потому что проверка всегда точечная.

### entity_versions

```sql
PRIMARY KEY ((entity_key))
```

Эта таблица нужна для защиты от out-of-order событий. Consumer хранит последний
`sequence_number` по логической сущности:

```sql
SELECT last_sequence_number
FROM entity_versions
WHERE entity_key = ?;
```

`entity_key` является partition key, потому что порядок проверяется отдельно для
каждого товара или заказа.

### event_history_by_product

```sql
PRIMARY KEY ((product_id), event_time, event_id)
WITH CLUSTERING ORDER BY (event_time DESC, event_id ASC)
```

Эта таблица нужна для аудита событий по товару:

```sql
SELECT *
FROM event_history_by_product
WHERE product_id = ?;
```

`product_id` выбран partition key, потому что историю обычно смотрят по
конкретному SKU. `event_time` и `event_id` являются clustering key: события
внутри товара сортируются по времени, а `event_id` делает ключ уникальным даже
если два события имеют одинаковое время.

### orders_by_id

```sql
PRIMARY KEY ((order_id))
```

Эта таблица хранит текущее состояние заказа. Consumer читает заказ по `order_id`
при обработке `ORDER_COMPLETED`, поэтому `order_id` является partition key.

## Обоснование consistency level для чтений

В проекте используются:

```text
cassandra_read_consistency = QUORUM
cassandra_write_consistency = QUORUM
```

Keyspace создается с replication factor 3:

```sql
WITH replication = {
  'class': 'NetworkTopologyStrategy',
  'dc1': 3
}
```

Для чтений выбран `QUORUM`, а не `ONE`, потому что обработка складских
событий использует read-modify-write:
1. consumer читает текущий остаток;
2. вычисляет новый остаток;
3. записывает обновленное состояние.

Если читать с `ONE`, можно быстрее получить ответ, но есть риск прочитать
устаревшее значение с одной отставшей реплики. Для склада это опасно: остаток
может быть пересчитан от старого числа.

`QUORUM` медленнее, чем `ONE`, потому что чтение ждет большинство реплик, но
дает более сильную консистентность. При `RF=3` с `QUORUM` выполняется правило:

```text
read quorum + write quorum > replication factor
2 + 2 > 3
```

Это значит, что множество реплик, подтвердивших запись, и множество реплик,
участвующих в чтении, пересекаются хотя бы в одной реплике. Поэтому чтение с
`QUORUM` лучше подходит для корректного пересчета остатков.

`ALL` не выбран, потому что он требовал бы ответа от всех трех реплик. При
падении одной Cassandra-ноды чтения и записи перестали бы работать, а пункт 8
задания требует продолжать обработку событий при остановке одной ноды.

Итоговый trade-off:

- `ONE` быстрее, но слабее по консистентности;
- `QUORUM` немного медленнее, но дает баланс консистентности и отказоустойчивости;
- `ALL` строже, но не выдерживает падение одной ноды.

Для этого проекта выбран `QUORUM`, потому что складские остатки важнее читать
согласованно, а кластер с `RF=3` все еще продолжает работать при недоступности
одной ноды.
