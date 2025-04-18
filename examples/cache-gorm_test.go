package examples

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ahuigo/gofnext"
	"github.com/ahuigo/gofnext_pg"
)

// List cache: select key,encode(value,'escape') from gofnext_cache_map ;
const DSN = "host=localhost user=role1 password='' dbname=testdb port=5432 sslmode=disable TimeZone=Asia/Shanghai"

func TestPgCacheClient(t *testing.T) {
	// method 1: by default: localhost:6379
	cache := gofnext_pg.NewCachePg("func-name")

	// method 2: set pg dsn
	cache.SetPgDsn(DSN)

	// method 3: set pg db(gorm)
	// cache.SetPgDb()
	cache.SetMaxHashKeyLen(0)
	cache.SetMaxHashKeyLen(100)
}

func TestPgCacheFuncWithTTL(t *testing.T) {
	// Original function
	executeCount := 0
	getUserScore := func(more int) (int, error) {
		executeCount++
		return 98 + more, nil
	}

	// Cacheable Function
	getUserScoreFromDbWithCache := gofnext.CacheFn1Err(getUserScore, &gofnext.Config{
		TTL:      time.Hour,
		CacheMap: gofnext_pg.NewCachePg("getUser").SetPgDsn(DSN).ClearCache(),
	})

	// Execute the function multi times in parallel.
	for i := 0; i < 10; i++ {

		score, err := getUserScoreFromDbWithCache(1)
		if err != nil || score != 99 {
			t.Errorf("score should be 99, but get %d", score)
		}
		score, _ = getUserScoreFromDbWithCache(2)
		if score != 100 {
			t.Fatalf("score should be 100, but get %d", score)
		}
		getUserScoreFromDbWithCache(3)
		getUserScoreFromDbWithCache(3)
	}

	if executeCount != 3 {
		t.Errorf("executeCount should be 1, but get %d", executeCount)
	}
}

func TestPgCacheFuncWithReuseTTL(t *testing.T) {
	// Original function
	executeCount := atomic.Int32{}
	getNum := func(more int) (int, error) {
		executeCount.Add(1)
		return int(executeCount.Load()) + more, nil
	}

	// Cacheable Function
	ttl := time.Millisecond * 100
	reuseTTL := time.Millisecond * 50
	getNumWithCache := gofnext.CacheFn1Err(getNum, &gofnext.Config{
		TTL:      ttl,
		ReuseTTL: reuseTTL,
		CacheMap: gofnext_pg.NewCachePg("getNum").SetPgDsn(DSN).ClearCache(),
	})

	// 1. init cache: count=1
	num, _ := getNumWithCache(0)
	gofnext.AssertEqual(t, num, 1)
	gofnext.AssertEqual(t, executeCount.Load(), 1)

	// 2. wait ttl: count=2(async run)
	time.Sleep(ttl)
	num, _ = getNumWithCache(0)
	gofnext.AssertEqual(t, num, 1)
	time.Sleep(time.Millisecond)
	gofnext.AssertEqual(t, executeCount.Load(), 2)

	// wait reuseTTL+ttl: count=3
	time.Sleep(reuseTTL + ttl)
	num, _ = getNumWithCache(0)
	gofnext.AssertEqual(t, num, 3)
	gofnext.AssertEqual(t, executeCount.Load(), 3)
}

func TestPgCacheFuncWithNoTTL(t *testing.T) {
	// Original function
	executeCount := 0
	getUserScore := func(more int, flag bool) (int, error) {
		executeCount++
		return 98 + more, nil
	}

	// Cacheable Function
	getUserScoreFromDbWithCache := gofnext.CacheFn2Err(
		getUserScore,
		&gofnext.Config{
			CacheMap: gofnext_pg.NewCachePg("getuser-nottl").SetPgDsn(DSN).ClearCache(),
		},
	)

	// Execute the function multi times in parallel.
	parallelCall(func() {
		score, err := getUserScoreFromDbWithCache(1, true)
		if err != nil || score != 99 {
			t.Errorf("score should be 99, but get %d", score)
		}
		getUserScoreFromDbWithCache(2, true)
		getUserScoreFromDbWithCache(3, true)
		getUserScoreFromDbWithCache(3, true)
	}, 10)

	if executeCount != 3 {
		t.Errorf("executeCount should be 3, but get %d", executeCount)
	}
}

func TestPgCacheFuncWithTTLTimeout(t *testing.T) {
	// Original function
	executeCount := 0
	getUserScore := func(more int) (int, error) {
		executeCount++
		return 98 + more, nil
	}

	// Cacheable Function
	getUserScoreFromDbWithCache := gofnext.CacheFn1Err(getUserScore, &gofnext.Config{
		TTL:      time.Millisecond * 200,
		CacheMap: gofnext_pg.NewCachePg("getuser-with-ttl").SetPgDsn(DSN).ClearCache(),
	})

	// Execute the function multi times in parallel.
	for i := 0; i < 5; i++ { //5 times
		getUserScoreFromDbWithCache(1) // 1.5ms
		getUserScoreFromDbWithCache(1) // 0.2ms(read cache from postgres)
		time.Sleep(time.Millisecond * 200)
	}

	if executeCount != 5 {
		t.Errorf("executeCount should be 5, but get %d", executeCount)
	}
}

func parallelCall(fn func(), times int) {
	var wg sync.WaitGroup
	for k := 0; k < times; k++ {
		wg.Add(1)
		go func() {
			fn()
			wg.Done()
		}()
	}
	wg.Wait()
}
