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
