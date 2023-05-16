package entigorm

import (
	"context"
	"errors"
	"strings"

	"gorm.io/gorm"
)

type Entity[E entity] interface {
	QueryMaker[E]
	QueryConsumer[E]
	RawExecutor[E]
	Transactor[E]
}

type QueryMaker[E entity] interface {
	Where(Clause) Entity[E]
	Having(Clause) Entity[E]
	Select(cols ...string) Entity[E]
	Offset(int) Entity[E]
	Limit(int) Entity[E]
	OrderBy(string) Entity[E]
	Ascending() Entity[E]
	Descending() Entity[E]
	GroupBy(string) Entity[E]
	Join(sql string, args []any) Entity[E]
}

type QueryConsumer[E entity] interface {
	Find(context.Context) ([]E, error)
	One(context.Context) error
	Insert(ctx context.Context, commit bool) error
	Update(ctx context.Context, commit bool) error
	Delete(ctx context.Context, commit bool) error
}

type RawExecutor[E entity] interface {
	Query(sql string, values ...any) error
	QueryRows(sql string, values ...any) ([]E, error)
	Exec(sql string, values ...any) error
}

type Transactor[E entity] interface {
	Tx() Transaction
	SetTx(Transaction) Entity[E]
}

type entity interface {
	Table() string
}

type Transaction interface {
	implement()
}

type transaction struct {
	db        *gorm.DB
	savePoint string
	orderBy   string
}

func (t *transaction) implement() {}

type SQLEntity[E entity] struct {
	transaction *transaction
	error       error
	entity      E
	clause      Clause
}

func SQL[E entity](ent E) Entity[E] {
	return &SQLEntity[E]{
		entity:      ent,
		transaction: &transaction{db: db.Begin()},
	}
}

func (e *SQLEntity[E]) Select(cols ...string) Entity[E] {
	e.transaction.db = e.transaction.db.Select(cols)

	return e
}

func (e *SQLEntity[E]) Where(whereClause Clause) Entity[E] {
	e.transaction.db = e.transaction.db.Where(whereClause.ToSQL())

	return e
}

func (e *SQLEntity[E]) OrderBy(name string) Entity[E] {
	e.transaction.orderBy = name
	e.transaction.db = e.transaction.db.Order(name)

	return e
}

func (e *SQLEntity[E]) Ascending() Entity[E] {
	if len(e.transaction.orderBy) == 0 {
		panic("call OrderBy before Ascending")
	}

	e.transaction.db = e.transaction.db.Order(e.transaction.orderBy + ASCOperator)

	return e
}

func (e *SQLEntity[E]) Descending() Entity[E] {
	if len(e.transaction.orderBy) == 0 {
		panic("call OrderBy before Descending")
	}

	e.transaction.orderBy, _ = strings.CutSuffix(
		e.transaction.orderBy,
		ASCOperator,
	)

	e.transaction.db = e.transaction.db.Order(e.transaction.orderBy + DESCOperator)

	return e
}

func (e *SQLEntity[E]) Offset(value int) Entity[E] {
	e.transaction.db = e.transaction.db.Offset(value)

	return e
}

func (e *SQLEntity[E]) Limit(value int) Entity[E] {
	e.transaction.db = e.transaction.db.Limit(value)

	return e
}

func (e *SQLEntity[E]) GroupBy(name string) Entity[E] {
	e.transaction.db = e.transaction.db.Group(name)

	return e
}

func (e *SQLEntity[E]) Having(whereClause Clause) Entity[E] {
	e.clause = whereClause
	e.transaction.db = e.transaction.db.Having(e.clause.ToSQL())

	return e
}

func (e *SQLEntity[E]) Join(sql string, args []any) Entity[E] {
	e.transaction.db = e.transaction.db.Joins(sql, args...)

	return e
}

func (e *SQLEntity[E]) Find(ctx context.Context) ([]E, error) {
	result := make([]E, 0)

	err := e.transaction.db.
		WithContext(ctx).
		Find(&result).
		Error
	if err != nil {
		return nil, e.joinError(err)
	}

	return result, err
}

func (e *SQLEntity[E]) One(ctx context.Context) error {
	err := e.transaction.db.
		WithContext(ctx).
		First(&e.entity).
		Error
	if err != nil {
		return e.joinError(err)
	}

	return nil
}

func (e *SQLEntity[E]) Insert(ctx context.Context, commit bool) error {
	err := e.transaction.db.
		WithContext(ctx).
		Create(&e.entity).
		Error
	if err != nil {
		e.error = err

		return e.joinError(e.rollback())
	}

	if commit {
		return e.commit()
	}

	return nil
}

func (e *SQLEntity[E]) Update(ctx context.Context, commit bool) error {
	err := e.transaction.db.
		WithContext(ctx).
		Updates(&e.entity).
		Error
	if err != nil {
		e.error = err

		return e.joinError(e.rollback())
	}

	if commit {
		return e.commit()
	}

	return nil
}

func (e *SQLEntity[E]) Delete(ctx context.Context, commit bool) error {
	err := e.transaction.db.
		WithContext(ctx).
		Delete(&e.entity).
		Error
	if err != nil {
		e.error = err

		return e.joinError(e.rollback())
	}

	if commit {
		return e.commit()
	}

	return nil
}

func (e *SQLEntity[E]) SetTx(db Transaction) Entity[E] {
	if e.transaction.db != nil {
		panic("must call SetTx before every method")
	}

	e.transaction.db = db.(*transaction).db

	return e
}

func (e *SQLEntity[E]) Tx() Transaction {
	return e.transaction
}

func (e *SQLEntity[E]) Query(sql string, values ...any) error {
	err := e.transaction.db.
		Raw(sql, values...).
		Scan(&e.entity).Error
	if err != nil {
		return e.joinError(err)
	}

	return nil
}

func (e *SQLEntity[E]) QueryRows(sql string, values ...any) ([]E, error) {
	result := make([]E, 0)

	err := e.transaction.db.
		Raw(sql, values...).
		Scan(&result).Error
	if err != nil {
		return nil, e.joinError(err)
	}

	return result, nil
}

func (e *SQLEntity[E]) Exec(sql string, values ...any) error {
	err := e.transaction.db.Exec(sql, values...).Error
	if err != nil {
		return e.joinError(err)
	}

	return nil
}

func (e *SQLEntity[E]) commit() error {
	err := e.transaction.db.Commit().Error
	if err != nil {
		return e.joinError(err)
	}

	return nil
}

func (e *SQLEntity[E]) rollback() error {
	if len(e.transaction.savePoint) > 0 {
		err := e.transaction.db.RollbackTo(e.transaction.savePoint).Error

		return e.joinError(err)
	}

	err := e.transaction.db.Rollback().Error

	return e.joinError(err)
}

func (e *SQLEntity[E]) joinError(err error) error {
	if errors.Unwrap(e.error) != nil {
		return errors.Join(e.error, err)
	}

	e.error = err

	return e.error
}
