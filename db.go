package entigorm

import (
	"gorm.io/gorm"
)

var db *gorm.DB

func Init(gormdb *gorm.DB) {
	db = gormdb
}
