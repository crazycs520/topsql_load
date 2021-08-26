package main

import (
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql" // mysql driver
)

var (
	BeginTimestamp  = flag.Int64("begin-ts", time.Now().Unix(), "begin unix timestamp in second. Can be set an early ts to import tons of data, or a recent ts to import data per minute.")
	SQLCount        = flag.Int("sql-count", 200, "tag count per second")
	SQLChangeMinute = flag.Int("sql-change-minute", 1, "sql change minute")
	InstanceCount   = flag.Uint("instance-count", 1, "instance count")
	ThreadCount     = flag.Int("thread", 1, "thread count for concurrency")
	Minutes         = flag.Uint("minute", 1, "duration, the unit is minute")
	Command         = flag.String("cmd", "", "command")
	InstanceIDs     = flag.String("instance-ids", "", "the instance id array that use to query, split by ,")
	QueryTopN       = flag.Uint("top-n", 20, "query top-n")
	QueryWindowSize = flag.Uint("window", 60, "query window-size")

	DBAddr   = flag.String("db-addr", "127.0.0.1:4000", "database address")
	DBUser   = flag.String("db-user", "root", "database user")
	DBPasswd = flag.String("db-passwd", "", "database password")
	DBSchema = flag.String("db-database", "test", "default database")
)

func main() {
	flag.Parse()
	client := newTopSQLClient(*ThreadCount, &DBConnection{
		addr:     *DBAddr,
		user:     *DBUser,
		passwd:   *DBPasswd,
		database: *DBSchema,
	})
	switch *Command {
	case "prepare":
		startTime := time.Now()
		start_ts, end_ts := getTimeRange()
		fmt.Printf("start to prepare data, start_ts: %v, end_ts: %v, sql_count: %v\n", *BeginTimestamp, end_ts, *SQLCount)
		err := client.PrepareData(*InstanceCount, start_ts, end_ts, *SQLCount, *SQLChangeMinute)
		if err != nil {
			fmt.Printf("%v\n", err)
			os.Exit(-1)
		}
		fmt.Printf("prepare data finish, cost %s\n", time.Since(startTime))
	case "prepare-agg":
		startTime := time.Now()
		start_ts, end_ts := getTimeRange()
		fmt.Printf("start to prepare agg data, start_ts: %v, end_ts: %v\n", *BeginTimestamp, end_ts)
		err := client.PrepareAggData(*InstanceCount, start_ts, end_ts)
		if err != nil {
			fmt.Printf("%v\n", err)
			os.Exit(-1)
		}
		fmt.Printf("prepare agg data finish, cost %s\n", time.Since(startTime))
	case "query", "query-agg":
		start_ts, end_ts := getTimeRange()
		var instanceIDs []int
		var err error
		if *InstanceCount != 0 {
			for i := 1; i <= int(*InstanceCount); i++ {
				instanceIDs = append(instanceIDs, i)
			}
		}
		if len(instanceIDs) == 0 {
			instanceIDs, err = parseIntArray(*InstanceIDs)
			if err != nil {
				fmt.Printf("parse instance id error: %v\n", err)
				os.Exit(-1)
			}
		}
		err = client.QueryTopNSQL(instanceIDs, *QueryWindowSize, *QueryTopN, start_ts, end_ts, *Command == "query-agg")
		if err != nil {
			fmt.Printf("query error: %v\n", err)
			os.Exit(-1)
		}
	default:
		fmt.Printf("unknow cmd: %v\n", *Command)
	}
}

func parseIntArray(value string) ([]int, error) {
	idstrs := strings.Split(value, ",")
	ids := make([]int, 0, len(idstrs))
	for _, str := range idstrs {
		str = strings.TrimSpace(str)
		id, err := strconv.Atoi(str)
		if err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, nil
}

func getTimeRange() (start, end int64) {
	end_ts := time.Unix(*BeginTimestamp, 0).Add(time.Minute * time.Duration(*Minutes)).Unix()
	return *BeginTimestamp, end_ts
}
