package mls2

import (
	"database/sql"
	"fmt"
	_ "github.com/bmizerany/pq"
	"io"
	"mls2/logger"
	"reflect"
	"strings"
)

func OpenDB(connString string) (dbRef *sql.DB, err error) {
	dbRef, err = sql.Open("postgres", connString)
	if err != nil {
		logger.LogEvent("fatal", "database_connection_error", "error", err.Error())
	}
	return
}

type pgTable struct {
	parent *LockstepServer
	name string
	types map[string]reflect.Kind
	loaded bool
}

type LockstepServer struct {
	db *sql.DB
	tables map[string]*pgTable
}

func LockstepStream(db *sql.DB, w io.Writer, tableName string) error {
	finished := make(chan bool)
	c, err := lockstepQuery(db, tableName, finished)
	if err != nil {
		return err
	}
	for s := range c {
		_, err = w.Write([]byte(s))
		if err != nil {
			finished <- true
			return err
		}
	}
	return nil
}

func lockstepQuery(db *sql.DB, tableName string, finished chan bool) (chan string, error) {
	c := make(chan string)
	go func(db *sql.DB, table string, c chan string) {
		defer close(c)
		rows, err := startLockstepQuery(db, tableName)
		if err != nil {
			// no good way to send this error back?
			c <- err.Error()
			return
		}

		// Figure out columns
		cols, _ := rows.Columns()
		for i, _ := range cols {
			cols[i] = strings.ToLower(cols[i])
		}

		res := make(map[string]interface{}, len(cols))
		var fargs []interface{}

		for _, name := range cols {
			res[name] = new(interface{})
			fargs = append(fargs, res[name])
		}

		// debug output
		for i, col := range cols {
			fmt.Printf("Column: %v %s\n", i, col)
		}
		fmt.Printf("Columns: %v\n", )

		for rows.Next() {
// 			var name string
// 			var txidSnapshotXmin int64
// 			err := rows.Scan(&name, &txidSnapshotXmin)
			err := rows.Scan(fargs...)
			fmt.Printf("Scanned rows: %v, %s, %s, %s\n", fargs, fargs[0], fargs[1], fargs[3])
			if err != nil {
				// no good way to send this error back?
				c <- err.Error()
				return
			}
			c <- "fake" //name
		}
	}(db, tableName, c)
	return c, nil
}

func (l *LockstepServer) Stream(w io.Writer, tableName string) error {
	finished := make(chan bool)
	c, err := l.Query(tableName, finished)
	if err != nil {
		return err
	}
	for s := range c {
		_, err = w.Write([]byte(s))
		if err != nil {
			finished <- true
			return err
		}
	}
	return nil
}

func (l *LockstepServer) Query(tableName string, finished chan bool) (chan string, error) {
	if l.tables[tableName] == nil {
		return nil, fmt.Errorf("invalid tableName: %q", tableName)
	}
	t := l.tables[tableName]
	if !t.loaded {
		// Table schema is not loaded, we need it before we can query
		err := t.loadSchema()
		if err != nil {
			return nil, err
		}
	}

	c := make(chan string)
	go func(t *pgTable, c chan string) {
		defer close(c)
		rows, err := t.startLockstepQuery()
		if err != nil {
			// no good way to send this error back?
			c <- err.Error()
			return
		}

		// Figure out columns
		cols, _ := rows.Columns()
		for i, _ := range cols {
			cols[i] = strings.ToLower(cols[i])
		}

		res := make(map[string]interface{}, len(cols))
		var fargs []interface{}

		for _, name := range cols {
			if name == "txid_snapshot_xmin" {
				res[name] = new(int64)
				fargs = append(fargs, res[name])
			} else {
				res[name] = reflect.ValueOf(t.types[name]).Interface()
				fargs = append(fargs, reflect.ValueOf(t.types[name]).Interface())
			}
		}
		fmt.Printf("fargs before: %v\n", fargs)
		fmt.Printf("%v\n", reflect.ValueOf(fargs[1]))

		// debug output
		for i, col := range cols {
			fmt.Printf("Column: %v %s\n", i, col)
		}
		fmt.Printf("Columns: %v\n", )

		for rows.Next() {
// 			var name string
// 			var txidSnapshotXmin int64
// 			err := rows.Scan(&name, &txidSnapshotXmin)
			err := rows.Scan(fargs...)
			fmt.Printf("Scanned rows: %v, %s, %s, %s\n", fargs, fargs[0], fargs[1], fargs[3])
			if err != nil {
				// no good way to send this error back?
				c <- err.Error()
				return
			}
// 			for _, name := range columns {
// 				strval := fmt.Sprintf("%s", *res[name])
// 
// 				switch t.types[name] {
// 				case reflect.Uint64:
// 					intval, _ := strconv.Atoi(strval)
// 					item[name] = uint64(intval)
// 				case reflect.Int64:
// 					intval, _ := strconv.Atoi(strval)
// 					item[name] = intval
// 				case reflect.Float64:
// 					floatval, _ := strconv.ParseFloat(strval, 10)
// 					item[name] = floatval
// 				default:
// 					item[name] = strval
// 				}
// 			}
			c <- "fake" //name
		}
	}(t, c)
	return c, nil
}

func (l *LockstepServer) loadTables() error {
	l.tables = make(map[string]*pgTable)

	names, err := getTables(l.db)
	if err != nil {
		return err
	}
	for _, name := range names {
		l.tables[name] = &pgTable{parent: l, name: name}
	}
	for _, t := range l.tables {
		err = t.loadSchema()
		if err != nil {
			return err
		}
	}
	return nil
}

func (t *pgTable) loadSchema() error {
	r, err := describeTable(t.parent.db, t.name)
	if err != nil {
		return err
	}
	t.types = r
	return nil
}

func getTables(db *sql.DB) ([]string, error) {
	var tables []string
	var table string
	rows, err := db.Query("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
	if err != nil {
		return nil, err
	}

	for rows.Next() {
		rows.Scan(&table)
		tables = append(tables, table)
	}

	return tables, nil
}

func describeTable(db *sql.DB, name string) (map[string]reflect.Kind, error) {
	rows, err := db.Query(fmt.Sprintf("SELECT column_name, data_type FROM information_schema.columns WHERE table_name = '%s'", name))
	if err != nil {
		return nil, err
	}

	types := make(map[string]reflect.Kind)

	var colName, dtype string
	for rows.Next() {
		err := rows.Scan(&colName, &dtype)
		if err != nil {
			return nil, fmt.Errorf("Error describing table %s: %v", name, err.Error())
		}

		types[strings.ToLower(colName)] = getType(dtype)
	}

	return types, nil
}

func getType(pgtype string) reflect.Kind {
	switch pgtype {
	case "character", "character varying", "text":
		return reflect.String
	case "smallint", "integer", "bigint", "serial", "bigserial":
		return reflect.Int64
	case "boolean":
		return reflect.Bool
	// don't know how to deal w/ time..
// 	case "time", "timetz", "timestamp", "timestamptz":
// 		return reflect.ValueOf(&time.Time{}).Kind()
	default:
		fmt.Printf("Unknown type: %s\n", pgtype)
	}
	return reflect.String
}

func startLockstepQuery(db *sql.DB, tableName string) (*sql.Rows, error) {
	return db.Query(fmt.Sprintf("SELECT txid_snapshot_xmin(txid_current_snapshot()), name, * FROM %s", tableName))
}

func (t *pgTable) startLockstepQuery() (*sql.Rows, error) {
	return t.parent.db.Query(fmt.Sprintf("SELECT txid_snapshot_xmin(txid_current_snapshot()), name, * FROM %s", t.name))
}
