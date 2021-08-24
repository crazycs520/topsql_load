package main

import (
	"database/sql"
	"fmt"
	"os"
	"sync"
)

type DBConnection struct {
	addr     string
	user     string
	passwd   string
	database string

	sync.Mutex
	cache []*sql.DB
}

func (c *DBConnection) GetSQLClient() (cli *sql.DB) {
	c.Lock()
	if l := len(c.cache); l > 0 {
		cli = c.cache[l-1]
		c.cache = c.cache[:l-1]
		c.Unlock()
		return cli
	}
	c.Unlock()
	return c.newSQLClient()
}

func (c *DBConnection) PutSQLClient(cli *sql.DB) {
	c.Lock()
	c.cache = append(c.cache, cli)
	c.Unlock()
}

func (c *DBConnection) newSQLClient() *sql.DB {
	dbDSN := fmt.Sprintf("%s:%s@tcp(%s)/%s", c.user, c.passwd, c.addr, c.database)
	db, err := sql.Open("mysql", dbDSN)
	if err != nil {
		fmt.Println("can not connect to database.err: ", err.Error())
		os.Exit(1)
	}
	db.SetMaxOpenConns(1)
	return db
}
