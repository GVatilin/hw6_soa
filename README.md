# HW6 SOA

## Проектирование модели данных 

### 1. inventory_by_product_zone

partition key выбран product_id, потому что основной сценарий - читать остатки конкретного товара
clustering key выбран zone_id, чтобы различать и быстро получать зоны внутри одного товара

### 2. inventory_by_product

partition key выбран product_id, потому что таблица хранит агрегированный остаток по товару
clustering key не нужен, так как на один товар хранится одна строка

### 3. inventory_by_zone

partition key выбран zone_id, потому что основной сценарий - получить все товары в конкретной зоне
clustering key выбран product_id, чтобы различать товары внутри зоны

### 4. processed_events

partition key выбран event_id, потому что нужен быстрый точечный поиск для проверки идемпотентности

### 5. orders_by_id

partition key выбран order_id, потому что заказ читается по своему идентификатору

### 6. event_history_by_product

partition key выбран product_id, потому что история читается по товару
clustering key выбран как event_timestamp, event_id, чтобы хранить события товара в хронологическом порядке

## Уровень консистентности для чтения из БД

Для чтений используется ONE, потому что в этом приложении важны скорость и доступность чтения, а основная гарантия целостности обеспечивается QUORUM на записи.

## Schema evolution

Для event используется Avro + Schema Registry с BACKWARD.
В проекте есть две версии схемы:

warehouse_event_v1.avsc — без supplier_id
warehouse_event_v2.avsc — с полем supplier_id, у которого значение по умолчанию null

Обе версии регистрируются в Schema Registry автоматически при старте проекта. Producer может отправлять события V1 и V2 в один topic warehouse-events, а consumer обрабатывает обе версии без ошибок.

В consumer поле supplier_id нормализуется:

для V1 - null

для V2 - берётся из события

### Как добавить новую версию:

Добавить новую схему warehouse_event_v3.avsc
Изменять схему только backward-compatible способом.
Для новых полей задавать значение по умолчанию, новая версия автоматически зарегистрируется в Schema Registry.