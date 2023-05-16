package entigorm

import (
	"fmt"
)

type Clause interface {
	BoolClause
	EqualityClause

	IN(filed string, value any) Clause
	Like(filed string, value any) Clause
	Between(filed string, value any) Clause

	ToSQL() (sql string, args []any)
}

type BoolClause interface {
	AND() Clause
	OR() Clause
	NOT() Clause
}

type EqualityClause interface {
	EQ(filed string, value any) Clause
	GT(filed string, value any) Clause
	GTE(filed string, value any) Clause
	LT(filed string, value any) Clause
	LTE(filed string, value any) Clause
}

type Builer struct {
	key        string
	value      any
	operator   string
	nextBoolOP string
}

type Clauser struct {
	builder []Builer
	not     bool
}

func (w *Clauser) EQ(field string, value any) *Clauser {
	if w.not {
		field = NOTOperator + field
	}

	w.builder = append(w.builder, EQ(field, value).builder...)
	w.not = false

	return w
}

func (w *Clauser) GT(field string, value any) *Clauser {
	if w.not {
		field = NOTOperator + field
	}

	w.builder = append(w.builder, GT(field, value).builder...)
	w.not = false

	return w
}

func (w *Clauser) GTE(field string, value any) *Clauser {
	if w.not {
		field = NOTOperator + field
	}

	w.builder = append(w.builder, GTE(field, value).builder...)
	w.not = false

	return w
}

func (w *Clauser) LT(field string, value any) *Clauser {
	if w.not {
		field = NOTOperator + field
	}

	w.builder = append(w.builder, LT(field, value).builder...)
	w.not = false

	return w
}

func (w *Clauser) LTE(field string, value any) *Clauser {
	if w.not {
		field = NOTOperator + field
	}

	w.builder = append(w.builder, LTE(field, value).builder...)
	w.not = false

	return w
}

func (w *Clauser) IN(field string, values []any) *Clauser {
	if w.not {
		field = NOTOperator + field
	}

	w.builder = append(w.builder, IN(field, values...).builder...)
	w.not = false

	return w
}

func (w *Clauser) Like(field, value string) *Clauser {
	if w.not {
		field = NOTOperator + field
	}

	w.builder = append(w.builder, Like(field, value).builder...)
	w.not = false

	return w
}

func (w *Clauser) Between(field string, value any) *Clauser {
	if w.not {
		field = NOTOperator + field
	}

	w.builder = append(w.builder, Between(field, value).builder...)
	w.not = false

	return w
}

func (w *Clauser) AND() *Clauser {
	w.builder[len(w.builder)-1].nextBoolOP = ANDOperator

	return w
}

func (w *Clauser) OR() *Clauser {
	w.builder[len(w.builder)-1].nextBoolOP = OROperator

	return w
}

func (w *Clauser) NOT() *Clauser {
	w.not = true

	return w
}

func (w *Clauser) ToSQL() (string, []any) {
	args := make([]any, len(w.builder))

	for i, clause := range w.builder {
		args[i] = clause.value
	}

	var where string
	for _, clause := range w.builder {
		where += fmt.Sprintf("%s %s ?", clause.key, clause.operator)

		if len(clause.nextBoolOP) > 0 {
			where += " " + clause.nextBoolOP
		}
	}

	return where, args
}

func EQ(field string, value any) *Clauser {
	return makeWhereClause(EQOperator, field, value)
}

func GTE(field string, value any) *Clauser {
	return makeWhereClause(GTEOperator, field, value)
}

func GT(field string, value any) *Clauser {
	return makeWhereClause(GTOperator, field, value)
}

func LTE(field string, value any) *Clauser {
	return makeWhereClause(LTEOperator, field, value)
}

func LT(field string, value any) *Clauser {
	return makeWhereClause(LTOperator, field, value)
}

func Between(field string, value any) *Clauser {
	return makeWhereClause(BetWeen, field, value)
}

func Like(field, value string) *Clauser {
	return makeWhereClause(LikeOperator, field, value)
}

func IN(field string, values ...any) *Clauser {
	return makeWhereClause(INOperator, field, values)
}

func NOT() *Clauser {
	return &Clauser{
		not: true,
	}
}

func makeWhereClause(operator, field string, value any) *Clauser {
	return &Clauser{
		builder: []Builer{
			{
				key:      field,
				value:    value,
				operator: operator,
			},
		},
	}
}

const (
	EQOperator   = "="
	GTOperator   = ">"
	GTEOperator  = ">="
	LTOperator   = "<"
	LTEOperator  = "<="
	INOperator   = "IN"
	LikeOperator = "LIKE"
	BetWeen      = "BETWEEN"
	NOTOperator  = "NOT "
	OROperator   = "OR "
	ANDOperator  = "AND "
	ASCOperator  = " ASC"
	DESCOperator = " DESC"
)
