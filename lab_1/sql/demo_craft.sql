-- Синтетические учебные данные магазина для воспроизводимой проверки загрузки.
-- При совпадении ключа существующая запись источника сохраняется.
INSERT INTO source.craft_market_wide VALUES
                                         (1,1,'Анна Мастер','СПб','1985-01-10','anna@example.test',1,'Чашка','Керамика','Посуда',1000,1,'2024-04-01','2024-04-05','done',1,'Иван Покупатель','СПб','1990-05-10','ivan@example.test'),
                                         (2,1,'Анна Мастер','СПб','1985-01-10','anna@example.test',2,'Ваза','Керамика','Посуда',2000,2,'2024-04-02',NULL,'created',1,'Иван Покупатель','СПб','1990-05-10','ivan@example.test'),
                                         (3,1,'Анна Мастер','СПб','1985-01-10','anna@example.test',1,'Чашка','Керамика','Посуда',1000,3,'2024-05-01','2024-05-03','done',1,'Иван Покупатель','СПб','1990-05-10','ivan@example.test')
    ON CONFLICT DO NOTHING;
INSERT INTO source.craft_market_masters_products VALUES
    (1,'Борис Мастер','Москва','1980-03-20','boris@example.test',1,'Брошь','Серебро','Украшения',3000)
    ON CONFLICT DO NOTHING;
INSERT INTO source.craft_market_orders_customers VALUES
                                                     (1,1,1,'2024-04-03',NULL,'in_progress',2,'Ольга Покупатель','Москва','1992-02-14','olga@example.test'),
                                                     (2,1,1,'2024-04-04',NULL,'delivery',2,'Ольга Покупатель','Москва','1992-02-14','olga@example.test')
    ON CONFLICT DO NOTHING;
INSERT INTO source.craft_market_craftsmans VALUES
                                               (1,'Анна Мастер','СПб','1985-01-10','anna@example.test'),
                                               (2,'Вера Мастер','Казань','1988-11-02','vera@example.test')
    ON CONFLICT DO NOTHING;
INSERT INTO source.craft_market_customers VALUES
                                              (1,'Иван Покупатель','СПб','1990-05-10','ivan@example.test'),
                                              (3,'Пётр Покупатель','Казань','1995-06-12','petr@example.test')
    ON CONFLICT DO NOTHING;
INSERT INTO source.craft_market_orders VALUES
                                           (1,1,1,1,'2024-04-06','2024-04-12','done','Чашка','Керамика','Посуда',1000),
                                           (2,2,2,3,'2024-04-07',NULL,'created','Шарф','Шерсть','Одежда',1500)
    ON CONFLICT DO NOTHING;






-- Дополнительные учебные данные для проверки месячной витрины CDM.
-- Добавляем нового мастера и покупателя, не изменяя существующие записи.
INSERT INTO source.craft_market_craftsmans
(craftsman_id, craftsman_name, craftsman_address, craftsman_birthday, craftsman_email)
VALUES
    (3, 'Игорь Мастер', 'Екатеринбург', '1987-07-15', 'igor@example.test')
    ON CONFLICT DO NOTHING;



INSERT INTO source.craft_market_customers
(customer_id, customer_name, customer_address, customer_birthday, customer_email)
VALUES
    (2, 'Елена Покупатель', 'Москва', '1993-08-21', 'elena@example.test')
    ON CONFLICT DO NOTHING;



-- Новые заказы имеют ID больше исходных (1 и 2):
-- инкрементальный загрузчик STG сможет их забрать по order_id.
INSERT INTO source.craft_market_orders
(order_id, product_id, craftsman_id, customer_id,
 order_created_date, order_completion_date, order_status,
 product_name, product_description, product_type, product_price)
VALUES
-- Апрель: у Ивана три завершённых заказа (6, 4, 8 дней), медиана = 6.
(3, 3, 1, 1, '2024-04-10', '2024-04-14', 'done',        'Тарелка', 'Керамика', 'Посуда',    1800),
(4, 4, 2, 1, '2024-04-11', NULL,         'in_progress', 'Шарф',    'Шерсть',  'Одежда',    2200),
(5, 5, 1, 1, '2024-04-12', NULL,         'delivery',    'Кувшин',  'Керамика','Посуда',    1200),
(6, 6, 2, 1, '2024-04-12', '2024-04-20', 'done',        'Кольцо',  'Серебро', 'Украшения', 4000),
(7, 7, 2, 3, '2024-04-15', '2024-04-19', 'done',        'Палантин','Шерсть',  'Одежда',    1600),
(8, 8, 1, 3, '2024-04-16', NULL,         'created',     'Чашка',   'Керамика','Посуда',     900),
-- Май: тот же покупатель в новом отчётном периоде, плюс другие покупатели.
(9, 9, 1, 1, '2024-05-03', '2024-05-09', 'done',        'Ваза',    'Керамика','Посуда',    1500),
(10,10,2, 1, '2024-05-06', '2024-05-11', 'done',        'Браслет', 'Серебро', 'Украшения', 2500),
(11,11,2, 1, '2024-05-08', NULL,         'in_progress', 'Серьги',  'Серебро', 'Украшения', 3500),
(12,12,1, 3, '2024-05-09', NULL,         'delivery',    'Тарелка', 'Керамика','Посуда',    1100),
(13,13,3, 2, '2024-05-10', '2024-05-12', 'done',        'Подвеска','Серебро', 'Украшения', 3200),
(14,14,3, 2, '2024-05-11', NULL,         'created',     'Шарф',    'Шерсть',  'Одежда',     800),
-- Июнь: ещё один месяц для проверки группировки.
(15,15,1, 2, '2024-06-01', '2024-06-07', 'done',        'Кувшин',  'Керамика','Посуда',    2000),
(16,16,3, 2, '2024-06-03', '2024-06-13', 'done',        'Кольцо',  'Серебро', 'Украшения', 4000)
    ON CONFLICT DO NOTHING;



