package gofnext_pg

import (
	"database/sql"
	"os"
	"time"

	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

var db *gorm.DB
var SqlDb *sql.DB

func GetTestDb() *gorm.DB {
	var err error
	dsn := "host=localhost user=role1 password='' dbname=testdb port=5432 sslmode=disable TimeZone=Asia/Shanghai"
	//dsn:="host=localhost user=gorm password=gorm dbname=gorm port=9920 sslmode=disable TimeZone=Asia/Shanghai"
	db, err = gorm.Open(postgres.Open(dsn), &gorm.Config{
		// Logger: newLogger,
		// Logger: logger.Default.LogMode(logger.Info),
	})
	if err != nil {
		panic(err)
	}

	// Db = Db.Debug()

	SqlDb, _ = db.DB()
	// defer sqlDB.Close()
	// SetMaxIdleConns 设置空闲连接池中连接的最大数量
	SqlDb.SetMaxIdleConns(10)
	// SetMaxOpenConns 设置打开数据库连接的最大数量。
	SqlDb.SetMaxOpenConns(100)
	// SetConnMaxLifetime 设置了连接可复用的最大时间。
	SqlDb.SetConnMaxLifetime(time.Hour)

	//Debug
	if os.Getenv("DEBUG") == "true" {
		db = db.Debug()
	}

	return db
}
