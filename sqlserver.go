package sqlserver

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"

	_ "github.com/denisenkom/go-mssqldb"
	"gorm.io/gorm"
	"gorm.io/gorm/callbacks"
	"gorm.io/gorm/clause"
	"gorm.io/gorm/logger"
	"gorm.io/gorm/migrator"
	"gorm.io/gorm/schema"
)

type Config struct {
	DriverName        string
	DSN               string
	DefaultStringSize int
	Conn              gorm.ConnPool
	ProductVersion    string
	Edition           string
}

type Dialector struct {
	*Config
}

func (dialector Dialector) Name() string {
	return "sqlserver"
}

func Open(dsn string) gorm.Dialector {
	return &Dialector{Config: &Config{DSN: dsn}}
}

func New(config Config) gorm.Dialector {
	return &Dialector{Config: &config}
}

func (dialector Dialector) Initialize(db *gorm.DB) (err error) {

	// register callbacks
	callbacks.RegisterDefaultCallbacks(db, &callbacks.Config{})
	db.Callback().Create().Replace("gorm:create", Create)

	if dialector.DriverName == "" {
		dialector.DriverName = "sqlserver"
	}

	if dialector.Conn != nil {
		db.ConnPool = dialector.Conn
	} else {
		db.ConnPool, err = sql.Open(dialector.DriverName, dialector.DSN)
		if err != nil {
			return err
		}
	}

	// retrieve the server version to determine if legacy queries should be used
	var version, edition string
	err = db.ConnPool.QueryRowContext(context.Background(), "SELECT SERVERPROPERTY('productversion') AS version, SERVERPROPERTY('Edition') AS edition;").Scan(&version, &edition)

	if err != nil {
		return errors.New(fmt.Sprintf("unable to get server version with error: %s", err.Error()))
	}

	db.Logger.Info(context.Background(), fmt.Sprintf("found server with version: %s %s", edition, version))
	dialector.ProductVersion = version
	dialector.Edition = edition

	if dialector.IsUnsupportedSQLServer() {
		db.Logger.Warn(context.Background(), fmt.Sprintf("this version of SQL server (%s) is unsupported. some backwards compatability has been implemented but may be incomplete", version))
	}

	for k, v := range dialector.ClauseBuilders() {
		db.ClauseBuilders[k] = v
	}
	return
}

func (dialector Dialector) ClauseBuilders() map[string]clause.ClauseBuilder {
	if dialector.IsUnsupportedSQLServer() {
		return dialector.getUnsupportedClauses()
	} else {
		return map[string]clause.ClauseBuilder{
			"LIMIT": func(c clause.Clause, builder clause.Builder) {
				if limit, ok := c.Expression.(clause.Limit); ok {
					if stmt, ok := builder.(*gorm.Statement); ok {
						if _, ok := stmt.Clauses["ORDER BY"]; !ok {
							if stmt.Schema != nil && stmt.Schema.PrioritizedPrimaryField != nil {
								builder.WriteString("ORDER BY ")
								builder.WriteQuoted(stmt.Schema.PrioritizedPrimaryField.DBName)
								builder.WriteByte(' ')
							} else {
								builder.WriteString("ORDER BY (SELECT NULL) ")
							}
						}
					}

					if limit.Offset > 0 {
						builder.WriteString("OFFSET ")
						builder.WriteString(strconv.Itoa(limit.Offset))
						builder.WriteString(" ROWS")
					}

					if limit.Limit > 0 {
						if limit.Offset == 0 {
							builder.WriteString("OFFSET 0 ROW")
						}
						builder.WriteString(" FETCH NEXT ")
						builder.WriteString(strconv.Itoa(limit.Limit))
						builder.WriteString(" ROWS ONLY")
					}
				}
			},
		}
	}
}

func (dialector Dialector) DefaultValueOf(field *schema.Field) clause.Expression {
	return clause.Expr{SQL: "NULL"}
}

func (dialector Dialector) Migrator(db *gorm.DB) gorm.Migrator {
	return Migrator{migrator.Migrator{Config: migrator.Config{
		DB:                          db,
		Dialector:                   dialector,
		CreateIndexAfterCreateTable: true,
	}}}
}

func (dialector Dialector) BindVarTo(writer clause.Writer, stmt *gorm.Statement, v interface{}) {
	writer.WriteString("@p")
	writer.WriteString(strconv.Itoa(len(stmt.Vars)))
}

func (dialector Dialector) QuoteTo(writer clause.Writer, str string) {
	writer.WriteByte('"')
	if strings.Contains(str, ".") {
		for idx, str := range strings.Split(str, ".") {
			if idx > 0 {
				writer.WriteString(`."`)
			}
			writer.WriteString(str)
			writer.WriteByte('"')
		}
	} else {
		writer.WriteString(str)
		writer.WriteByte('"')
	}
}

var numericPlaceholder = regexp.MustCompile("@p(\\d+)")

func (dialector Dialector) Explain(sql string, vars ...interface{}) string {
	for idx, v := range vars {
		if b, ok := v.(bool); ok {
			if b {
				vars[idx] = 1
			} else {
				vars[idx] = 0
			}
		}
	}

	return logger.ExplainSQL(sql, numericPlaceholder, `'`, vars...)
}

func (dialector Dialector) DataTypeOf(field *schema.Field) string {
	switch field.DataType {
	case schema.Bool:
		return "bit"
	case schema.Int, schema.Uint:
		var sqlType string
		switch {
		case field.Size < 16:
			sqlType = "smallint"
		case field.Size < 31:
			sqlType = "int"
		default:
			sqlType = "bigint"
		}

		if field.AutoIncrement {
			return sqlType + " IDENTITY(1,1)"
		}
		return sqlType
	case schema.Float:
		return "float"
	case schema.String:
		size := field.Size
		hasIndex := field.TagSettings["INDEX"] != "" || field.TagSettings["UNIQUE"] != ""
		if (field.PrimaryKey || hasIndex) && size == 0 {
			if dialector.DefaultStringSize > 0 {
				size = dialector.DefaultStringSize
			} else {
				size = 256
			}
		}
		if size > 0 && size <= 4000 {
			return fmt.Sprintf("nvarchar(%d)", size)
		}
		return "nvarchar(MAX)"
	case schema.Time:
		return "datetimeoffset"
	case schema.Bytes:
		return "varbinary(MAX)"
	}

	return string(field.DataType)
}

func (dialectopr Dialector) SavePoint(tx *gorm.DB, name string) error {
	tx.Exec("SAVE TRANSACTION " + name)
	return nil
}

func (dialectopr Dialector) RollbackTo(tx *gorm.DB, name string) error {
	tx.Exec("ROLLBACK TRANSACTION " + name)
	return nil
}

type Edition int

const (
	Enterprise Edition = iota + 1
	Business
	Developer
	Express
	ExpressAdvanced
	Standard
	Web
	Azure
	AzureEdge
	AzureEdgeDeveloper
	Unknown
)

func (dialector Dialector) IsUnsupportedSQLServer() bool {
	if major, _, edition, _, err := dialector.GetVersionAndType(); err == nil && major < 11 && edition < Azure {
		return true
	} else {
		return false
	}
}

func (dialector Dialector) GetVersionAndType() (versionMajor int, versionMinor int, edition Edition, is64Bit bool, err error) {
	if dialector.ProductVersion == "" {
		return 0, 0, 0, false, errors.New("no product edition provided")
	}

	if dialector.Edition == "" {
		return 0, 0, 0, false, errors.New("no product edition provided")
	}

	versionParts := strings.Split(dialector.ProductVersion, ".")
	if len(versionParts) > 0 {
		versionMajor, err = strconv.Atoi(versionParts[0])

		if err != nil {
			return 0, 0, 0, false, errors.New(fmt.Sprintf("invalid product version with error: %s", err.Error()))
		}
	}

	if len(versionParts) > 1 {
		versionMinor, _ = strconv.Atoi(versionParts[1]) // ignore any errors as the minor isn't hugely important
	}

	edStr := dialector.Edition
	is64Identifier := "(64-bit)"
	if strings.Contains(edStr, is64Identifier) {
		is64Bit = true
		edStr = strings.TrimSpace(strings.ReplaceAll(edStr, is64Identifier, ""))
	}

	// sourced from https://docs.microsoft.com/en-us/sql/t-sql/functions/serverproperty-transact-sq
	switch edStr {
	case "Enterprise Edition", "Enterprise Edition: Core-based Licensing", "Enterprise Evaluation Edition":
		edition = Enterprise
	case "Business Intelligence Edition":
		edition = Business
	case "Developer Edition":
		edition = Developer
	case "Express Edition":
		edition = Express
	case "Express Edition with Advanced Services":
		edition = ExpressAdvanced
	case "Standard Edition":
		edition = Standard
	case "Web Edition":
		edition = Web
	case "SQL Azure":
		edition = Azure
	case "Azure SQL Edge":
		edition = AzureEdge
	case "Azure SQL Edge Developer":
		edition = AzureEdgeDeveloper
	default:
		edition = Unknown
	}

	return versionMajor, versionMinor, edition, is64Bit, nil
}

func (Dialector) getUnsupportedClauses() map[string]clause.ClauseBuilder {
	return map[string]clause.ClauseBuilder{
		"SELECT": func(c clause.Clause, builder clause.Builder) {
			builder.WriteString("SELECT ")

			limit, offset, err := getLimitAndOffsetIfExists(builder)

			if err == nil {
				if offset == 0 {
					builder.WriteString(fmt.Sprintf("TOP(%d) ", limit))
				}
			}

			c.Expression.(clause.Select).Build(builder)
		},
		// due to legacy mssql syntax not having native support for offsets, a composite query is required.
		// this FROM clause wraps the LIMIT and ORDER BY clauses around original request which becomes a subquery
		"FROM": func(c clause.Clause, builder clause.Builder) {
			builder.WriteString("FROM ")
			if stmt, ok := builder.(*gorm.Statement); ok {
				limit, offset, err := getLimitAndOffsetIfExists(builder)

				if err == nil {
					// check whether we require offsetting
					if offset > 0 {
						// get the current selects and add row number selector
						builder.WriteString("(")
						if selects := stmt.Selects; len(selects) > 0 {
							for _, s := range selects {
								builder.WriteQuoted(s)
								builder.WriteByte(',')
							}
							builder.WriteString("ROW_NUMBER()")
						} else {
							builder.WriteString("SELECT *, ROW_NUMBER()")
						}

						builder.WriteString(" OVER (ORDER BY ")
						if orderBy, ok := stmt.Clauses["ORDER BY"]; ok {
							ob, _ := orderBy.Expression.(clause.OrderBy)
							ob.Build(builder)
						} else {
							if stmt.Schema != nil && stmt.Schema.PrioritizedPrimaryField != nil {
								builder.WriteQuoted(stmt.Schema.PrioritizedPrimaryField.DBName)
								builder.WriteByte(' ')
							} else {
								builder.WriteString("(SELECT NULL) ")
							}
						}
						builder.WriteString(") AS row ")
						if from, ok := stmt.Clauses["FROM"]; ok {
							from.Build(builder)
						} else {
							builder.WriteString("FROM ")
							builder.WriteQuoted(stmt.Table)
						}

						builder.WriteString(") a")

						var query []string
						var queryParam []int
						if offset > 0 {
							query = append(query, "row > ?")
							queryParam = append(queryParam, offset)
						}

						if limit > 0 {
							query = append(query, "row <= ?")
							if offset == 0 {
								queryParam = append(queryParam, limit)
							} else {
								queryParam = append(queryParam, limit+offset)
							}
						}

						for i := 0; i < len(query); i++ {
							if conds := stmt.BuildCondition(query[i], queryParam[i]); len(conds) > 0 {
								stmt.AddClause(clause.Where{conds})
							}
						}

						return
					}
				}
			}

			c.Expression.(clause.From).Build(builder)
		},
		"LIMIT": func(c clause.Clause, builder clause.Builder) {
			// handled by the from function
		},
		"ORDER BY": func(c clause.Clause, builder clause.Builder) {
			// handled by the from function when offsetting. if not, handle it here
			if _, offset, err := getLimitAndOffsetIfExists(builder); offset == 0 || err != nil {
				builder.WriteString("ORDER BY ")
				c.Expression.(clause.OrderBy).Build(builder)
			}
		},
	}
}

func getLimitAndOffsetIfExists(builder clause.Builder) (limit, offset int, err error) {
	if stmt, ok := builder.(*gorm.Statement); ok {
		if limit, ok := stmt.Clauses["LIMIT"]; ok {
			if limitExpr, ok := limit.Expression.(clause.Limit); ok {
				return limitExpr.Limit, limitExpr.Offset, nil
			}
		}
	}

	return 0, 0, errors.New("no limit / offset found")
}
