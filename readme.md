# gofnext_pg
gofnext's cache decorator for postgres.

## Cache with TTL
Refer to: [decorator example](https://github.com/ahuigo/gofnext_pg/blob/main/examples/)

```golang
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
```



