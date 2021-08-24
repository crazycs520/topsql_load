# Usage

## build
```shell
go build .
```

## load data

```
./topsql_load --cmd=prepare --minute=120 --instance-count=5 --thread=16 --db-addr='0.0.0.0:4000'
```

## query top-n

```sql
./topsql_load --cmd=query --instance-count=5 --begin-ts=1629807734 --minute=120 --db-addr='0.0.0.0:4000'
```

# Schema design

Some meta information should be cache in client to improve performance.

```sql
CREATE TABLE IF NOT EXISTS instance_meta (
  id bigint auto_increment,
  ip varchar(100),
  primary key (instance_id),
  unique index ip(ip)
);
```


```sql
CREATE TABLE IF NOT EXISTS sql_meta (
	id BIGINT auto_increment NOT NULL,
	sql_digest VARBINARY(100) NOT NULL,
	normalized_sql LONGTEXT NOT NULL,
	PRIMARY KEY (id),
	UNIQUE (sql_digest)
);
```

```sql
CREATE TABLE IF NOT EXISTS plan_meta (
	id BIGINT auto_increment NOT NULL,
	plan_digest VARBINARY(32) NOT NULL,
	normalized_plan LONGTEXT NOT NULL,
	PRIMARY KEY (id),
	UNIQUE (plan_digest)
);
```

```sql
CREATE TABLE IF NOT EXISTS instance_{instance_id}_metrics (
	sql_id BIGINT NOT NULL,
	plan_id BIGINT,
	timestamp BIGINT NOT NULL,
	cpu_time_ms INT NOT NULL
);
```
