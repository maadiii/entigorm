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
	OrderBy(name string, desc bool) Entitier[E]
	GroupBy(string) Entitier[E]
	ToSQL() []any
	IsMany() Entitier[E]
	Join([]any) Entitier[E]
}

type QueryConsumer[E entity] interface {
	Find(context.Context) ([]E, error)
	One(context.Context) (E, error)
	Insert(ctx context.Context) (Transaction, error)
	Update(ctx context.Context) (Transaction, error)
	Delete(ctx context.Context) (Transaction, error)
}

type RawExecutor[E entity] interface {
	Query(sql string, values ...any) error
	QueryRows(sql string, values ...any) ([]E, error)
	Exec(sql string, values ...any) error
}

type Transactor[E entity] interface {
	Begin() Entitier[E]
	SetTx(Transaction) Entitier[E]
	Commit() Entitier[E]
}

type entity interface {
	TableName() string
}

type Transaction interface {
	implement()
}

type transaction struct {
	scopes    []func(*gorm.DB) *gorm.DB
	tx        *gorm.DB
	commit    bool
	savePoint string
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
		transaction: &transaction{scopes: make([]func(*gorm.DB) *gorm.DB, 0)},
		clause:      &Clause{builder: make([]Builer, 0)},
	}
}

func (e *Entity[E]) Select(cols ...string) Entitier[E] {
	if len(cols) > 1 {
		e.transaction.scopes = append(
			e.transaction.scopes,
			func(db *gorm.DB) *gorm.DB {
				return db.Select(cols)
			},
		)
	} else {
		e.transaction.scopes = append(
			e.transaction.scopes,
			func(db *gorm.DB) *gorm.DB {
				return db.Select(cols[0], cols[1])
			},
		)
	}

	return e
}

func (e *Entity[E]) Where(whereClause *Clause) Entitier[E] {
	e.clause = whereClause
	e.transaction.scopes = append(
		e.transaction.scopes,
		func(db *gorm.DB) *gorm.DB {
			args := whereClause.ToSQL()
			if len(args) > 1 {
				return db.Where(args[0], args[1:]...)
			}

			if len(args) > 0 {
				return db.Where(args[0])
			}

			return nil
		},
	)

	return e
}

func (e *Entity[E]) OrderBy(name string, ascending bool) Entitier[E] {
	e.transaction.scopes = append(
		e.transaction.scopes,
		func(db *gorm.DB) *gorm.DB {
			if ascending {
				return db.Order(name + " ASC ")
			}

			return db.Order(name + " DESC ")
		},
	)

	return e
}

func (e *Entity[E]) Offset(value int) Entitier[E] {
	e.transaction.scopes = append(
		e.transaction.scopes,
		func(db *gorm.DB) *gorm.DB {
			return db.Offset(value)
		},
	)

	return e
}

func (e *Entity[E]) Limit(value int) Entitier[E] {
	e.transaction.scopes = append(
		e.transaction.scopes,
		func(db *gorm.DB) *gorm.DB {
			return db.Limit(value)
		},
	)

	return e
}

func (e *Entity[E]) GroupBy(name string) Entitier[E] {
	e.transaction.scopes = append(
		e.transaction.scopes,
		func(db *gorm.DB) *gorm.DB {
			return db.Group(name)
		},
	)

	return e
}

func (e *Entity[E]) Having(whereClause *Clause) Entitier[E] {
	e.clause = whereClause
	e.transaction.scopes = append(
		e.transaction.scopes,
		func(db *gorm.DB) *gorm.DB {
			return db.Having(e.clause.ToSQL())
		},
	)

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
		e.transaction.scopes = append(
			e.transaction.scopes,
			func(db *gorm.DB) *gorm.DB {
				return db.Joins(table, db.Where(stmt, args[2:]))
			},
		)
	} else {
		e.transaction.scopes = append(
			e.transaction.scopes,
			func(db *gorm.DB) *gorm.DB {
				return db.Preload(table)
			},
		)
	}

	return e
}

func (e *Entity[E]) Find(ctx context.Context) ([]E, error) {
	result := make([]E, 0)

	err := db.Scopes(e.transaction.scopes...).Find(&result).Error
	if err != nil {
		return nil, e.joinError(err)
	}

	return result, err
}

func (e *Entity[E]) One(ctx context.Context) (E, error) {
	var result E

	err := db.Scopes(e.transaction.scopes...).Find(&result).Error
	if err != nil {
		return result, e.joinError(err)
	}

	return result, nil
}

func (e *Entity[E]) Insert(ctx context.Context) (tx Transaction, err error) {
	if e.transaction.tx != nil {
		return e.txInsert(ctx)
	}

	return nil, db.Create(e.table).Error
}

func (e Entity[E]) txInsert(ctx context.Context) (tx Transaction, err error) {
	err = e.transaction.tx.Create(e.table).Error
	if err != nil {
		return e.rollback(err)
	}

	return e.commit()
}

func (e *Entity[E]) Update(ctx context.Context) (tx Transaction, err error) {
	if e.transaction.tx != nil {
		return e.txUpdate(ctx)
	}

	return nil, db.Scopes(e.transaction.scopes...).Updates(e.table).Error
}

func (e *Entity[E]) txUpdate(ctx context.Context) (tx Transaction, err error) {
	err = e.transaction.tx.Scopes(e.transaction.scopes...).Updates(e.table).Error
	if err != nil {
		return e.rollback(err)
	}

	return e.commit()
}

func (e *Entity[E]) Delete(ctx context.Context) (tx Transaction, err error) {
	if e.transaction.tx != nil {
		return e.txDelete(ctx)
	}

	return nil, db.Scopes(e.transaction.scopes...).Delete(e.table).Error
}

func (e *Entity[E]) txDelete(ctx context.Context) (tx Transaction, err error) {
	err = e.transaction.tx.Scopes(e.transaction.scopes...).Delete(e.table).Error
	if err != nil {
		return e.rollback(err)
	}

	return e.commit()
}

func (e *Entity[E]) Begin() Entitier[E] {
	e.transaction.tx = db.Begin()

	return e
}

func (e *Entity[E]) Commit() Entitier[E] {
	e.transaction.commit = true

	return e
}

func (e *Entity[E]) SetTx(tx Transaction) Entitier[E] {
	e.transaction.tx = tx.(*transaction).tx

	return e
}

func (e *Entity[E]) Query(sql string, values ...any) error {
	err := db.Scopes(e.transaction.scopes...).Raw(sql, values...).Scan(&e.table).Error
	if err != nil {
		return e.joinError(err)
	}

	return nil
}

func (e *Entity[E]) QueryRows(sql string, values ...any) ([]E, error) {
	result := make([]E, 0)

	err := db.Scopes(e.transaction.scopes...).Raw(sql, values...).Scan(&e.table).Error
	if err != nil {
		return nil, e.joinError(err)
	}

	return result, nil
}

func (e *Entity[E]) Exec(sql string, values ...any) error {
	err := db.Scopes(e.transaction.scopes...).Exec(sql, values...).Error
	if err != nil {
		return e.joinError(err)
	}

	return nil
}

func (e *Entity[E]) commit() (tx Transaction, err error) {
	if e.transaction.commit {
		return e.transaction, e.transaction.tx.Commit().Error
	}

	return e.transaction, nil
}

func (e *Entity[E]) rollback(err error) (Transaction, error) {
	if len(e.transaction.savePoint) > 0 {
		rErr := e.transaction.tx.RollbackTo(e.transaction.savePoint).Error
		if rErr != nil {
			e.error = rErr
		}

		return nil, e.joinError(err)
	}

	rErr := e.transaction.tx.Rollback().Error
	if rErr != nil {
		e.error = rErr
		return nil, e.joinError(err)
	}

	return e.transaction, nil
}

func (e *Entity[E]) joinError(err error) error {
	if errors.Unwrap(e.error) != nil {
		return errors.Join(e.error, err)
	}

	e.error = err

	return e.error
}
