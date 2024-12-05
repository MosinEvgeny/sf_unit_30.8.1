/*
    Схема для информационной системы
    отслеживания выполнения задач.
*/

DROP TABLE IF EXISTS tasks_labels, tasks, labels, users;

-- пользователи системы
CREATE TABLE IF NOT EXISTS users (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL
);

-- метки задач
CREATE TABLE IF NOT EXISTS labels (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL
);

-- задачи
CREATE TABLE IF NOT EXISTS tasks (
    id SERIAL PRIMARY KEY,
    -- время создания задачи (по умолчанию - текущее)
    opened BIGINT NOT NULL DEFAULT extract(epoch from now()),
    closed BIGINT DEFAULT 0, -- время завершения задачи
    author_id INTEGER REFERENCES users(id) DEFAULT 0, -- автор задачи
    assigned_id INTEGER REFERENCES users(id) DEFAULT 0, -- ответственный
    title TEXT NOT NULL, -- название задачи
    content TEXT NOT NULL -- текст задачи
);

-- связь многие-ко-многим между задачами и метками
CREATE TABLE IF NOT EXISTS tasks_labels (
    task_id INTEGER REFERENCES tasks(id),
    label_id INTEGER REFERENCES labels(id)
);

-- наполнение БД начальными данными
TRUNCATE TABLE users, labels CASCADE;
INSERT INTO users (id, name) VALUES (0, 'default');
INSERT INTO users (name) VALUES ('Мосин ЕВгений'), ('Вавилин Алексей'), ('Ширинкин Дмитрий');
INSERT INTO labels (id, name) VALUES (0, 'default');
INSERT INTO labels (name) VALUES ('Задача'), ('Ошибка');
INSERT INTO tasks (title, content) VALUES 
('Работа с БД в Go', 'Посмотреть и изучить краткое руководство по работе с PostgreSQL в Go'),
('Написать приложение БД', 'Для закрепления знаний необходимо самомстоятельно разработать приложение!');