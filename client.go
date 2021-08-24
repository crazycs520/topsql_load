package main

import (
	"bytes"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"fmt"
	"hash"
	"math/rand"
	"os"
	"strconv"
	"sync"
	"time"
	"topsql_load/lru"
)

type TopSQLClient struct {
	instanceCache *lru.SimpleLRUCache
	sqlMetaCache  *lru.SimpleLRUCache
	planMetaCache *lru.SimpleLRUCache

	concurrency int
	dbConn      *DBConnection
	instances   []InstanceMeta
}

func newTopSQLClient(concurrency int, dbConn *DBConnection) *TopSQLClient {
	client := &TopSQLClient{
		instanceCache: lru.NewSimpleLRUCache(1000),
		sqlMetaCache:  lru.NewSimpleLRUCache(100_0000),
		planMetaCache: lru.NewSimpleLRUCache(100_0000),
		concurrency:   concurrency,
		dbConn:        dbConn,
	}

	return client
}

func (c *TopSQLClient) PrepareData(instanceCount uint, start_ts int64, end_ts int64, sqlCount int) error {
	err := c.InitMetaSchema()
	if err != nil {
		return err
	}
	err = c.InitSchemaForInstance(int(instanceCount))
	if err != nil {
		return err
	}

	c.LoadMetricsData(start_ts, end_ts, sqlCount)
	return nil
}

func (c *TopSQLClient) QueryTopNSQL(instanceIDs []int, windowSize, topN uint, start_ts int64, end_ts int64) error {
	cli := c.dbConn.GetSQLClient()
	defer c.dbConn.PutSQLClient(cli)

	first := true
	for {
		for _, id := range instanceIDs {
			instance := InstanceMeta{id: id}
			query := fmt.Sprintf(queryTopNSQLQuery, windowSize, instance.getTableName(), start_ts, end_ts, topN)
			if first {
				fmt.Println(query)
				first = false
			}
			start := time.Now()
			rows, err := cli.Query(query)
			if err != nil {
				return err
			}
			for rows.Next() {
			}
			err = rows.Close()
			if err != nil {
				return err
			}
			fmt.Printf("query %v, start_ts: %v, minute: %.1f, topN: %v, cost: %v\n", instance.getTableName(), start_ts, (time.Second * time.Duration(end_ts-start_ts+1)).Minutes(), topN, time.Since(start))
		}
	}
}

var queryTopNSQLQuery = `SELECT time_window, cpu_time_agg, cpu_time_sum_in_window, sql_id
	FROM (SELECT *,
				 SUM(windowed.cpu_time_agg) OVER (PARTITION BY windowed.time_window ORDER BY windowed.cpu_time_agg DESC, windowed.sql_id DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS cpu_time_sum_in_window,
				 RANK() OVER (PARTITION BY windowed.time_window ORDER BY windowed.cpu_time_agg DESC, windowed.sql_id DESC) AS rk
		  FROM (SELECT sql_id,
					   cast(timestamp / %v as unsigned) AS time_window,
					   sum(cpu_time_ms) AS cpu_time_agg
				FROM %v
				WHERE timestamp >= %v
				  AND timestamp <  %v
				GROUP BY time_window, sql_id) windowed) topsql
	WHERE topsql.rk <= %v;`

func (c *TopSQLClient) InitMetaSchema() error {
	schema1 := `CREATE TABLE IF NOT EXISTS instance_meta (
  id bigint auto_increment,
  ip varchar(100),
   primary key (id),
   unique index ip(ip)
);`
	schema2 := `CREATE TABLE IF NOT EXISTS sql_meta (
	id BIGINT auto_increment NOT NULL,
	sql_digest VARBINARY(100) NOT NULL,
	normalized_sql LONGTEXT NOT NULL,
	PRIMARY KEY (id),
	UNIQUE (sql_digest)
);`
	schema3 := `CREATE TABLE IF NOT EXISTS plan_meta (
	id BIGINT auto_increment NOT NULL,
	plan_digest VARBINARY(32) NOT NULL,
	normalized_plan LONGTEXT NOT NULL,
	PRIMARY KEY (id),
	UNIQUE (plan_digest)
);`

	cli := c.dbConn.GetSQLClient()
	defer c.dbConn.PutSQLClient(cli)

	schemas := []string{schema1, schema2, schema3}
	for _, schema := range schemas {
		_, err := cli.Exec(schema)
		if err != nil {
			return err
		}
	}
	return nil
}

func (c *TopSQLClient) InitSchemaForInstance(instanceCount int) error {
	cli := c.dbConn.GetSQLClient()
	defer c.dbConn.PutSQLClient(cli)
	instances := make([]InstanceMeta, 0, instanceCount)
	for i := 1; i <= instanceCount; i++ {
		instance := InstanceMeta{
			ip: "instance_" + strconv.Itoa(i),
			id: i,
		}
		instances = append(instances, instance)
		schema := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
	sql_id BIGINT NOT NULL,
	plan_id BIGINT,
	timestamp BIGINT NOT NULL,
	cpu_time_ms INT NOT NULL
);`, instance.getTableName())

		_, err := cli.Exec(schema)
		if err != nil {
			return err
		}
	}

	buf := bytes.NewBuffer(make([]byte, 0, 4096))
	buf.WriteString("insert ignore instance_meta values ")
	for i, instance := range instances {
		if i > 0 {
			buf.WriteByte(',')
		}
		buf.WriteString(fmt.Sprintf("(%v,'%v')", instance.id, instance.ip))
	}
	_, err := cli.Exec(buf.String())
	if err != nil {
		return err
	}
	c.instances = instances
	return nil
}

type InstanceMeta struct {
	ip string
	id int
}

func (m *InstanceMeta) getTableName() string {
	return fmt.Sprintf("instance_%v_metrics", m.id)
}

type InsertMeta struct {
	sql        string
	plan       string
	sqlDigest  []byte
	planDigest []byte
	sqlID      int
	planID     int
}

func (c *TopSQLClient) LoadMetricsData(start_ts int64, end_ts int64, sqlCount int) {
	if c.concurrency < 1 {
		c.concurrency = 1
	}
	limitCh := make(chan struct{}, c.concurrency)
	step := int64(60)
	var wg sync.WaitGroup
	sqlBaseCount := 0
	for ts := start_ts; ts < end_ts; ts += step {
		start := ts
		end := start + step
		limitCh <- struct{}{}
		baseCount := sqlBaseCount
		wg.Add(1)
		go func() {
			defer func() {
				<-limitCh
				defer wg.Done()
			}()
			err := c.loadData(start, end, sqlCount, baseCount)
			if err != nil {
				fmt.Printf("%v", err)
				os.Exit(-1)
			}
		}()
		sqlBaseCount += sqlCount
	}
	wg.Wait()
}

func (c *TopSQLClient) loadData(start_ts, end_ts int64, sqlCount, sqlBaseCount int) error {
	cli := c.dbConn.GetSQLClient()
	defer c.dbConn.PutSQLClient(cli)

	metas := c.genInsertMeta(sqlCount, sqlBaseCount)

	err := c.insertSQLMeta(metas, cli)
	if err != nil {
		return err
	}
	err = c.insertPlanMeta(metas, cli)
	if err != nil {
		return err
	}

	for _, instance := range c.instances {
		err = c.insertMetrics(start_ts, end_ts, metas, instance, cli)
		if err != nil {
			return err
		}
	}
	return err
}

func (c *TopSQLClient) insertMetrics(start_ts, end_ts int64, metas []InsertMeta, instance InstanceMeta, cli *sql.DB) error {
	prepareBuf := bytes.NewBuffer(make([]byte, 0, 1024))
	prepareBuf.WriteString(fmt.Sprintf("insert into %s values ", instance.getTableName()))

	for j := 0; j < len(metas); j++ {
		if j > 0 {
			prepareBuf.WriteByte(',')
		}
		prepareBuf.WriteString("(?,?,?,?)")
	}
	stmt, err := cli.Prepare(prepareBuf.String())
	if err != nil {
		return err
	}

	args := make([]interface{}, 0, 4*len(metas))
	for ts := start_ts; ts < end_ts; ts++ {
		args = args[:0]
		for j := 0; j < len(metas); j++ {
			args = append(args, metas[j].sqlID, metas[j].planID, ts, rand.Intn(100))
		}
		_, err = stmt.Exec(args...)
		if err != nil {
			return err
		}
	}
	return err
}

func (c *TopSQLClient) insertSQLMeta(metas []InsertMeta, cli *sql.DB) error {
	buf := bytes.NewBuffer(make([]byte, 0, 4096))
	buf.WriteString("insert ignore sql_meta values ")
	for i, meta := range metas {
		if i > 0 {
			buf.WriteByte(',')
		}
		buf.WriteString(fmt.Sprintf("(%v,'%v','%v')", meta.sqlID, hex.EncodeToString(meta.sqlDigest), meta.sql))
	}
	_, err := cli.Exec(buf.String())
	return err
}

func (c *TopSQLClient) insertPlanMeta(metas []InsertMeta, cli *sql.DB) error {
	buf := bytes.NewBuffer(make([]byte, 0, 4096))
	buf.WriteString("insert ignore plan_meta values ")
	for i, meta := range metas {
		if i > 0 {
			buf.WriteByte(',')
		}
		buf.WriteString(fmt.Sprintf("(%v,'%v','%v')", meta.planID, hex.EncodeToString(meta.planDigest), meta.plan))
	}
	_, err := cli.Exec(buf.String())
	return err
}

func (c *TopSQLClient) genInsertMeta(sqlCount, sqlBaseCount int) []InsertMeta {
	metas := make([]InsertMeta, 0, sqlCount)
	hasher := sha256.New()
	for i := 1; i <= sqlCount; i++ {
		id := i + sqlBaseCount
		sql, plan := c.genSQLPlanMeta(id)
		sqlDigest := c.genDigest(sql, hasher)
		planDigest := c.genDigest(plan, hasher)
		metas = append(metas, InsertMeta{
			sql:        sql,
			plan:       plan,
			sqlDigest:  sqlDigest,
			planDigest: planDigest,
			sqlID:      id,
			planID:     id,
		})
	}
	return metas
}

func (c *TopSQLClient) genSQLPlanMeta(id int) (sql, plan string) {
	sql = "select * from table_" + strconv.Itoa(id)
	plan = "project -------> table_reader --------> table_scan_" + strconv.Itoa(id)
	return sql, plan
}

func (c *TopSQLClient) genDigest(str string, hasher hash.Hash) []byte {
	hasher.Reset()
	hasher.Write([]byte(str))
	return hasher.Sum(nil)
}