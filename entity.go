package entigorm

import (
	"context"
	"errors"
	"reflect"
	"strings"

	"gorm.io/gorm"
)

type Entitier[E entity] interface {
	QueryMaker[E]
	QueryConsumer[E]
	RawExecutor[E]
	Transactor[E]
}

type QueryMaker[E entity] interface {
	Where(*Clause) Entitier[E]
	Having(*Clause) Entitier[E]
	Select(cols ...string) Entitier[E]
	Offset(int) Entitier[E]
	Limit(int) Entitier[E]
	OrderBy(string) Entitier[E]
	Ascending() Entitier[E]
	Descending() Entitier[E]
	GroupBy(string) Entitier[E]
	ToSQL() []any
	IsMany() Entitier[E]
	Join([]any) Entitier[E]
}

type QueryConsumer[E entity] interface {
	Find(context.Context) ([]E, error)
	One(context.Context) (E, error)
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
	SetTx(Transaction) Entitier[E]
}

type entity interface {
	TableName() string
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

type Entity[E entity] struct {
	transaction *transaction
	error       error
	table       E
	clause      *Clause
	hasMany     bool
}

func SQL[E entity](ent E) Entitier[E] {
	return &Entity[E]{
		table:       ent,
		transaction: &transaction{db: db.Begin()},
		clause:      &Clause{builder: make([]Builer, 0)},
	}
}

func (e *Entity[E]) Select(cols ...string) Entitier[E] {
	if len(cols) > 1 {
		e.transaction.db = e.transaction.db.Select(cols)
	} else {
		e.transaction.db = e.transaction.db.Select(cols[0], cols[1:])
	}

	return e
}

func (e *Entity[E]) Where(whereClause *Clause) Entitier[E] {
	e.clause = whereClause

	return e
}

func (e *Entity[E]) OrderBy(name string) Entitier[E] {
	e.transaction.orderBy = name
	e.transaction.db = e.transaction.db.Order(name)

	return e
}

func (e *Entity[E]) Ascending() Entitier[E] {
	if len(e.transaction.orderBy) == 0 {
		panic("call OrderBy before Ascending")
	}

	e.transaction.db = e.transaction.db.Order(e.transaction.orderBy + ASCOperator)

	return e
}

func (e *Entity[E]) Descending() Entitier[E] {
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

func (e *Entity[E]) Offset(value int) Entitier[E] {
	e.transaction.db = e.transaction.db.Offset(value)

	return e
}

func (e *Entity[E]) Limit(value int) Entitier[E] {
	e.transaction.db = e.transaction.db.Limit(value)

	return e
}

func (e *Entity[E]) GroupBy(name string) Entitier[E] {
	e.transaction.db = e.transaction.db.Group(name)

	return e
}

func (e *Entity[E]) Having(whereClause *Clause) Entitier[E] {
	e.clause = whereClause
	e.transaction.db = e.transaction.db.Having(e.clause.ToSQL())

	return e
}

func (e *Entity[E]) IsMany() Entitier[E] {
	e.hasMany = true

	return e
}

func (e *Entity[E]) ToSQL() []any {
	var table string
	if e.hasMany {
		table = strings.Title(e.table.TableName())
	} else {
		table = reflect.ValueOf(e.table).Elem().Type().Name()
	}
	args := []any{table}

	if len(e.clause.ToSQL()) > 1 {
		args = append(args, e.clause.ToSQL()...)
	}

	return args
}

func (e *Entity[E]) Join(args []any) Entitier[E] {
	table := args[0].(string)

	if len(args) > 1 {
		query := args[1].(string)
		splited := strings.Split(query, " = ")

		var splitedStmt []string
		for _, s := range splited {
			words := strings.Split(s, " ")
			for _, word := range words {
				if len(word) > 0 {
					splitedStmt = append(splitedStmt, word)
				}
			}
		}

		for i := 1; i < len(splitedStmt); i++ {
			if splitedStmt[i] == "?" {
				splitedStmt[i-1] = table + "." + splitedStmt[i-1] + " ="
			}
		}

		stmt := strings.Join(splitedStmt, " ")
		e.transaction.db = e.transaction.db.Joins(table, db.Where(stmt, args[2:]))
	} else {
		e.transaction.db = e.transaction.db.Preload(table)
	}

	return e
}

func (e *Entity[E]) Find(ctx context.Context) ([]E, error) {
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

func (e *Entity[E]) One(ctx context.Context) (E, error) {
	var result E

	err := e.transaction.db.
		Find(&result, e.clause.ToSQL()...).
		Error
	if err != nil {
		return result, e.joinError(err)
	}

	return result, nil
}

func (e *Entity[E]) Insert(ctx context.Context, commit bool) error {
	err := e.transaction.db.
		WithContext(ctx).
		Create(&e.table).
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

func (e *Entity[E]) Update(ctx context.Context, commit bool) error {
	err := e.transaction.db.
		WithContext(ctx).
		Updates(&e.table).
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

func (e *Entity[E]) Delete(ctx context.Context, commit bool) error {
	err := e.transaction.db.
		WithContext(ctx).
		Delete(&e.table).
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

func (e *Entity[E]) SetTx(db Transaction) Entitier[E] {
	if e.transaction.db != nil {
		panic("must call SetTx before every method")
	}

	e.transaction.db = db.(*transaction).db

	return e
}

func (e *Entity[E]) Tx() Transaction {
	return e.transaction
}

func (e *Entity[E]) Query(sql string, values ...any) error {
	err := e.transaction.db.
		Raw(sql, values...).
		Scan(&e.table).Error
	if err != nil {
		return e.joinError(err)
	}

	return nil
}

func (e *Entity[E]) QueryRows(sql string, values ...any) ([]E, error) {
	result := make([]E, 0)

	err := e.transaction.db.
		Raw(sql, values...).
		Scan(&result).Error
	if err != nil {
		return nil, e.joinError(err)
	}

	return result, nil
}

func (e *Entity[E]) Exec(sql string, values ...any) error {
	err := e.transaction.db.Exec(sql, values...).Error
	if err != nil {
		return e.joinError(err)
	}

	return nil
}

func (e *Entity[E]) commit() error {
	err := e.transaction.db.Commit().Error
	if err != nil {
		return e.joinError(err)
	}

	return nil
}

func (e *Entity[E]) rollback() error {
	if len(e.transaction.savePoint) > 0 {
		err := e.transaction.db.RollbackTo(e.transaction.savePoint).Error

		return e.joinError(err)
	}

	err := e.transaction.db.Rollback().Error

	return e.joinError(err)
}

func (e *Entity[E]) joinError(err error) error {
	if errors.Unwrap(e.error) != nil {
		return errors.Join(e.error, err)
	}

	e.error = err

	return e.error
}
