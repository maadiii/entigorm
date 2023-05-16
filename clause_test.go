package entigorm_test

import (
	"testing"

	"entigorm"
	"github.com/stretchr/testify/assert"
)

func TestClauser_EQ(t *testing.T) {
	t.Run("field = ?", func(t *testing.T) {
		clauser := new(entigorm.Clauser)
		clause, args := clauser.EQ("field", "value").ToSQL()

		assert.Equal(t, "field = ?", clause)
		assert.Len(t, args, 1)
	})

	t.Run("NOT field = ?", func(t *testing.T) {
		clauser := new(entigorm.Clauser)
		clause, args := clauser.NOT().EQ("field", "value").ToSQL()

		assert.Equal(t, "NOT field = ?", clause)
		assert.Len(t, args, 1)
	})
}

func TestClauser_GT(t *testing.T) {
	t.Run("field > ?", func(t *testing.T) {
		clauser := new(entigorm.Clauser)
		clause, args := clauser.GT("field", "value").ToSQL()

		assert.Equal(t, "field > ?", clause)
		assert.Len(t, args, 1)
	})

	t.Run("NOT field > ?", func(t *testing.T) {
		clauser := new(entigorm.Clauser)
		clause, args := clauser.NOT().GT("field", "value").ToSQL()

		assert.Equal(t, "NOT field > ?", clause)
		assert.Len(t, args, 1)
	})

	t.Run("field >= ?", func(t *testing.T) {
		clauser := new(entigorm.Clauser)
		clause, args := clauser.GTE("field", "value").ToSQL()

		assert.Equal(t, "field >= ?", clause)
		assert.Len(t, args, 1)
	})

	t.Run("not greater and equal", func(t *testing.T) {
		clauser := new(entigorm.Clauser)
		clause, args := clauser.NOT().GTE("field", "value").ToSQL()

		assert.Equal(t, "NOT field >= ?", clause)
		assert.Len(t, args, 1)
	})
}

func TestClauser_LT(t *testing.T) {
	t.Run("lesser", func(t *testing.T) {
		clauser := new(entigorm.Clauser)
		clause, args := clauser.LT("field", "value").ToSQL()

		assert.Equal(t, "field < ?", clause)
		assert.Len(t, args, 1)
	})

	t.Run("not lesser", func(t *testing.T) {
		clauser := new(entigorm.Clauser)
		clause, args := clauser.NOT().LT("field", "value").ToSQL()

		assert.Equal(t, "NOT field < ?", clause)
		assert.Len(t, args, 1)
	})

	t.Run("lesser and equal", func(t *testing.T) {
		clauser := new(entigorm.Clauser)
		clause, args := clauser.LTE("field", "value").ToSQL()

		assert.Equal(t, "field <= ?", clause)
		assert.Len(t, args, 1)
	})

	t.Run("not lesser and equal", func(t *testing.T) {
		clauser := new(entigorm.Clauser)
		clause, args := clauser.NOT().LTE("field", "value").ToSQL()

		assert.Equal(t, "NOT field <= ?", clause)
		assert.Len(t, args, 1)
	})
}

func TestClauser_IN(t *testing.T) {
	t.Run("IN", func(t *testing.T) {
		clauser := new(entigorm.Clauser)
		clause, args := clauser.
			IN("field", []any{"value1", "value2"}).
			ToSQL()

		assert.Equal(t, "field IN ?", clause)
		assert.Len(t, args, 1)
	})

	t.Run("not greater", func(t *testing.T) {
		clauser := new(entigorm.Clauser)
		clause, args := clauser.NOT().GT("field", "value").ToSQL()

		assert.Equal(t, "NOT field > ?", clause)
		assert.Len(t, args, 1)
	})

	t.Run("greater and equal", func(t *testing.T) {
		clauser := new(entigorm.Clauser)
		clause, args := clauser.GTE("field", "value").ToSQL()

		assert.Equal(t, "field >= ?", clause)
		assert.Len(t, args, 1)
	})

	t.Run("not greater and equal", func(t *testing.T) {
		clauser := new(entigorm.Clauser)
		clause, args := clauser.NOT().GTE("field", "value").ToSQL()

		assert.Equal(t, "NOT field >= ?", clause)
		assert.Len(t, args, 1)
	})
}
