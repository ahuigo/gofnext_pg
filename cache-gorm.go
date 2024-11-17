package gofnext_pg

import (
	"encoding/json"
	"errors"
	"hash/fnv"
	"strconv"
	"sync"
	"time"

	"crypto/md5"
	"crypto/sha512"
	"encoding/hex"

	"github.com/ahuigo/gofnext"
	"github.com/ahuigo/gofnext/serial"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type CacheTable struct {
	Key   string `gorm:"primaryKey;type:varchar(2048);not null"`
	Value []byte `gorm:"type:bytea"`
}

type pgMap struct {
	mu            sync.Mutex
	pgDb          *gorm.DB
	ttl           time.Duration
	errTtl        time.Duration
	tableName     string
	maxHashKeyLen int
	funcKey       string
	Error         error
}

type pgData struct {
	Data      []byte
	Err       []byte
	CreatedAt time.Time
	// TTL       time.Duration
}

func NewCachePg(funcKey string) *pgMap {
	if funcKey == "" {
		panic("NewCachePg: funcKey can not be empty")
	}
	return &pgMap{
		tableName: "gofnext_cache_map",
		funcKey:   funcKey,
	}
}

func (m *pgMap) SetPgDsn(dsn string) *pgMap {
	// dsn := "host=localhost user=role1 password='' dbname=testdb port=5432 sslmode=disable TimeZone=Asia/Shanghai"
	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{
		// Logger: logger.Default.LogMode(logger.Info),
	})
	if err != nil {
		panic(err)
	}
	m.pgDb = db
	m.initTable()
	return m
}

func (m *pgMap) SetPgDb(db *gorm.DB) *pgMap {
	m.pgDb = db
	m.initTable()
	return m
}
func (m *pgMap) GetPgDb() *gorm.DB {
	return m.pgDb
}

func (m *pgMap) initTable() *pgMap {
	err := m.pgDb.Table(m.tableName).AutoMigrate(&CacheTable{})
	if err != nil {
		m.Error = err
		panic(err)
	}
	return m
}

func (m *pgMap) Table() *gorm.DB {
	return m.pgDb.Table(m.tableName)
}

func (m *pgMap) ClearCache() *pgMap {
	m.checkDb()
	m.Error = m.pgDb.Exec("delete from " + m.tableName).Error
	if m.Error != nil {
		println("gofnext postgre cache error:" + m.Error.Error())
	}
	return m
}

func (m *pgMap) DropCacheTable() *pgMap {
	m.checkDb()
	m.pgDb.Migrator().DropTable(m.tableName)
	return m
}
func (m *pgMap) checkDb() {
	if m.pgDb == nil {
		panic("please call SetPgDsn or SetPgDb first")
	}
}

func (m *pgMap) HashKeyFunc(key ...any) []byte {
	if len(key) == 0 {
		return nil
	} else if len(key) == 1 {
		return serial.Bytes(key[0], false)
	} else {
		return serial.Bytes(key, false)
	}
}

func (m *pgMap) strkey(key any) string {
	var r string
	switch rt := key.(type) {
	case string:
		r = rt
	default:
		r = serial.String(key, false)
	}
	if m.maxHashKeyLen > 0 && len(r) > m.maxHashKeyLen {
		if m.maxHashKeyLen <= 8 {
			h := fnv.New64a()
			_, _ = h.Write([]byte(r))
			return strconv.FormatUint(h.Sum64(), 16)
		} else if m.maxHashKeyLen <= 32 {
			hash := md5.Sum([]byte(r))
			r = hex.EncodeToString(hash[:])
		} else if m.maxHashKeyLen <= 64 {
			hash := sha512.Sum512_256([]byte(r))
			r = hex.EncodeToString(hash[:])
		} else {
			hash := sha512.Sum512([]byte(r))
			r = hex.EncodeToString(hash[:])
		}
	}
	return m.funcKey + r
}

func (m *pgMap) Store(key, value any, err error) {
	pkey := m.strkey(key)
	data, _ := json.Marshal(value)
	cacheData := pgData{
		Data: data,
		// TTL:  m.ttl,
	}
	if err != nil && m.errTtl <= 0 {
		return
	}
	if m.ttl > 0 || m.errTtl >= 0 {
		cacheData.CreatedAt = time.Now()
	}
	if err != nil {
		cacheData.Err = []byte(err.Error())
	}
	val, _ := json.Marshal(cacheData)
	p := CacheTable{Key: pkey, Value: []byte(val)}
	err = m.Table().
		Clauses(clause.OnConflict{
			Columns:   []clause.Column{{Name: "key"}},
			UpdateAll: true,
		}).
		Create(&p).Error
	if err != nil {
		println(err.Error())
	}
}

func (m *pgMap) Load(key any) (value any, existed bool, err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	pkey := m.strkey(key)
	vals := [][]byte{}
	err = m.Table().Select("value").Where("key=?", pkey).Scan(&vals).Error
	if err != nil {
		println(err.Error())
		return
	}
	if len(vals) == 0 {
		return
	}
	cacheData := pgData{}
	err = json.Unmarshal(vals[0], &cacheData)
	if err != nil {
		return
	}

	value = cacheData.Data
	if cacheData.Err != nil {
		err = errors.New(string(cacheData.Err))
	}
	if (m.ttl > 0 && time.Since(cacheData.CreatedAt) > m.ttl) ||
		(m.errTtl >= 0 && cacheData.Err != nil && time.Since(cacheData.CreatedAt) > m.errTtl) {
		return value, false, nil //expired
	}
	existed = true
	return
}

func (m *pgMap) SetTTL(ttl time.Duration) gofnext.CacheMap {
	m.ttl = ttl
	return m
}
func (m *pgMap) SetErrTTL(errTTL time.Duration) gofnext.CacheMap {
	m.errTtl = errTTL
	return m
}

func (m *pgMap) SetMaxHashKeyLen(l int) *pgMap {
	m.maxHashKeyLen = l
	return m
}

func (m *pgMap) NeedMarshal() bool {
	return true
}
