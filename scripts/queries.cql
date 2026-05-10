USE warehouse;

-- Получить остаток товара в конкретной зоне
SELECT * FROM inventory_by_product_zone WHERE product_id = 'SKU-001' AND zone_id = 'ZONE-A';

-- Получить все остатки товара по всем зонам
SELECT * FROM inventory_by_product_zone WHERE product_id = 'SKU-001';

-- Получить агрегированный остаток товара
SELECT * FROM inventory_by_product WHERE product_id = 'SKU-001';

-- Получить все товары в зоне
SELECT * FROM inventory_by_zone WHERE zone_id = 'ZONE-A';

-- Проверить идемпотентность/историю обработки
SELECT * FROM processed_events WHERE event_id = 'idem-001';
