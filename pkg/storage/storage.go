package postgres

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

type Storage struct {
	db *pgxpool.Pool
}

func New(connstr string) (*Storage, error) {
	db, err := pgxpool.New(context.Background(), connstr)
	if err != nil {
		return nil, err
	}
	s := Storage{
		db: db,
	}
	return &s, nil
}

// Задача
type Task struct {
	ID         int    `json:"id"`
	Opened     int64  `json:"opened"`
	Closed     int64  `json:"closed"`
	AuthorID   int    `json:"author_id"`
	AssignedID int    `json:"assigned_id"`
	Title      string `json:"title"`
	Content    string `json:"content"`
}

// Метка
type Label struct {
	ID   int    `json:"id"`
	Name string `json:"name"`
}

// Tasks - возвращает список всех задач из БД.
func (s *Storage) Tasks(taskID, authorID int) ([]Task, error) {
	rows, err := s.db.Query(context.Background(), `
	SELECT
		id,
		opened,
		closed,
		author_id,
		assigned_id,
		title,
		content
	FROM tasks
	WHERE
		($1 = 0 OR id = $1) AND
		($2 = 0 OR author_id = $2)
	ORDER BY id;
	`,
		taskID,
		authorID,
	)
	if err != nil {
		return nil, err
	}
	var tasks []Task
	for rows.Next() {
		var t Task
		err = rows.Scan(
			&t.ID,
			&t.Opened,
			&t.Closed,
			&t.AuthorID,
			&t.AssignedID,
			&t.Title,
			&t.Content,
		)
		if err != nil {
			return nil, err
		}
		tasks = append(tasks, t)
	}
	return tasks, rows.Err()
}

// NewTask создаёт новую задачу и возвращает её id.
func (s *Storage) NewTask(t Task) (int, error) {
	var id int
	err := s.db.QueryRow(context.Background(), `
	INSERT INTO tasks (title, content)
	VALUES ($1, $2) RETURNING id;
	`,
		t.Title,
		t.Content,
	).Scan(&id)
	return id, err
}

// GetTaskByID возвращает задачу по её ID.
func (s *Storage) GetTaskByID(id int) (*Task, error) {
	row := s.db.QueryRow(context.Background(), `
		SELECT
			id, opened, closed, author_id, assigned_id, title, content
		FROM tasks
		WHERE id = $1
	`, id)

	task := &Task{}
	err := row.Scan(&task.ID, &task.Opened, &task.Closed, &task.AuthorID, &task.AssignedID, &task.Title, &task.Content)
	if err == sql.ErrNoRows {
		return nil, fmt.Errorf("Задача с ID %d не найдена", id)
	} else if err != nil {
		return nil, fmt.Errorf("Ошибка в получении задачи: %w", err)
	}

	return task, nil
}

// UpdateTask обновляет задачу по id.
func (s *Storage) UpdateTask(t Task) error {
	_, err := s.db.Exec(context.Background(), `
		UPDATE tasks SET
			opened = $2,
			closed = $3,
			author_id = $4,
			assigned_id = $5,
			title = $6,
			content = $7
		WHERE id = $1;
	`, t.ID, t.Opened, t.Closed, t.AuthorID, t.AssignedID, t.Title, t.Content)
	return err
}

// DeleteTask удаляет задачу по id.
func (s *Storage) DeleteTask(id int) error {
	_, err := s.db.Exec(context.Background(), "DELETE FROM tasks WHERE id = $1", id)
	return err
}

// CreateTasks создает массив задач.
func (s *Storage) CreateTasks(tasks []Task) error {
	tx, err := s.db.Begin(context.Background())
	if err != nil {
		return fmt.Errorf("Ошибка при запуске транзакции: %w", err)
	}
	defer tx.Rollback(context.Background())

	for _, task := range tasks {
		_, err := tx.Exec(context.Background(), `
			INSERT INTO tasks (opened, closed, author_id, assigned_id, title, content)
			VALUES ($1, $2, $3, $4, $5, $6)
		`, task.Opened, task.Closed, task.AuthorID, task.AssignedID, task.Title, task.Content)
		if err != nil {
			return fmt.Errorf("Ошибка при вставке задачи: %w", err)
		}
	}

	return tx.Commit(context.Background())
}

// GetTasksByAuthor возвращает список задач по автору.
func (s *Storage) GetTasksByAuthor(authorID int) ([]Task, error) {
	return s.Tasks(0, authorID)
}

// AddLabelToTask добавляет метку к задаче.
func (s *Storage) AddLabelToTask(taskID, labelID int) error {
	_, err := s.db.Exec(context.Background(), `
		INSERT INTO tasks_labels (task_id, label_id)
		VALUES ($1, $2)
	`, taskID, labelID)
	return err
}

// RemoveLabelFromTask удаляет метку из задачи.
func (s *Storage) RemoveLabelFromTask(taskID, labelID int) error {
	_, err := s.db.Exec(context.Background(), `
		DELETE FROM tasks_labels
		WHERE task_id = $1 AND label_id = $2
	`, taskID, labelID)
	return err
}

// GetTasksByLabel возвращает список задач, имеющих указанную метку.
func (s *Storage) GetTasksByLabel(labelID int) ([]Task, error) {
	rows, err := s.db.Query(context.Background(), `
		SELECT t.id, t.opened, t.closed, t.author_id, t.assigned_id, t.title, t.content
		FROM tasks t
		JOIN tasks_labels tl ON t.id = tl.task_id
		WHERE tl.label_id = $1
	`, labelID)
	if err != nil {
		return nil, fmt.Errorf("Ошибка в получении задач по метке: %w", err)
	}
	defer rows.Close()

	var tasks []Task
	for rows.Next() {
		var task Task
		err := rows.Scan(&task.ID, &task.Opened, &task.Closed, &task.AuthorID, &task.AssignedID, &task.Title, &task.Content)
		if err != nil {
			return nil, fmt.Errorf("Ошибка сканирования строки задачи: %w", err)
		}
		tasks = append(tasks, task)
	}

	return tasks, rows.Err()
}

func (s *Storage) Close() {
	s.db.Close()
}