package tigris

import (
	"context"
	"crypto/tls"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"strings"

	"github.com/magiconair/properties"
	"github.com/pingcap/go-ycsb/pkg/prop"
	"github.com/pingcap/go-ycsb/pkg/ycsb"
	"github.com/tigrisdata/tigris-client-go/config"
	"github.com/tigrisdata/tigris-client-go/driver"
)

const (
	tigrisDBName     = "tigris.dbname"
	tigrisHost       = "tigris.host"
	tigrisPort       = "tigris.port"
	tigrisProtocol   = "tigris.protocol"
	tigrisCollName   = "tigris.collection"
	tigrisFieldCount = "tigris.fieldcount"
)

type tigrisDB struct {
	db driver.Database
}

type tigrisCreator struct {
}

var fieldCount int64

func (t *tigrisDB) InitThread(ctx context.Context, _ int, _ int) context.Context {
	// TODO: multiple connection here, otherwise it will hit only one tigris app server
	return ctx
}

func (t *tigrisDB) CleanupThread(_ context.Context) {
}

func (t *tigrisDB) read(ctx context.Context, collection string, startKey string, count int, fields []string) ([]map[string][]byte, error) {
	var projection string
	var doc driver.Document
	var res []map[string][]byte

	filter := fmt.Sprintf(`{ "Key": "%s" }`, startKey)
	if len(fields) == 0 {
		projection = `{}`
	} else {
		var included bool
		projection = `{ "Key": true`
		for i := int64(0); i < fieldCount; i++ {
			included = false
			currentFieldName := fmt.Sprintf("field%d", i)
			for _, field := range fields {
				if currentFieldName == field {
					included = true
				}
			}
			if included {
				projection = projection + fmt.Sprintf(`, "%s": true`, currentFieldName)
			} else {
				projection = projection + fmt.Sprintf(`, "%s": false`, currentFieldName)
			}
		}
		projection = projection + " }"
	}
	it, err := t.db.Read(ctx, collection, driver.Filter(filter), driver.Projection{}, &driver.ReadOptions{Limit: int64(count)})
	if err != nil {
		if os.Getenv("TIGRIS_PRINT_ERRORS") != "" {
			fmt.Println("got error during read: ", err.Error())
		}
	}
	defer it.Close()

	singleMap := make(map[string][]byte)
	for it.Next(&doc) {
		// Implementing limit with using stop reading the iterator
		_ = json.Unmarshal(doc, &res)
		res = append(res, singleMap)
	}
	return res, nil
}

func (t *tigrisDB) Read(ctx context.Context, collection string, key string, fields []string) (map[string][]byte, error) {
	res, err := t.read(ctx, collection, key, 1, fields)
	if err != nil {
		if os.Getenv("TIGRIS_PRINT_ERRORS") != "" {
			fmt.Println("got error during read: ", err.Error())
		}
	}
	if len(res) != 1 {
		return nil, nil
	}
	return res[0], nil
}

func (t *tigrisDB) Scan(ctx context.Context, collection string, startKey string, count int, fields []string) ([]map[string][]byte, error) {
	return t.read(ctx, collection, startKey, count, fields)
}

func (t *tigrisDB) Update(ctx context.Context, collection string, key string, values map[string][]byte) error {
	update := `{ "$set": {`
	for fieldName, fieldValue := range values {
		update = update + fmt.Sprintf(`"%s": "%s",`, fieldName, fieldValue)
	}
	update = strings.TrimRight(update, ",")
	update = update + ` } }`

	filter := fmt.Sprintf(`{ "Key": "%s" }`, key)
	_, err := t.db.Update(ctx, collection, driver.Filter(filter), driver.Update(update))
	if err != nil {
		if os.Getenv("TIGRIS_PRINT_ERRORS") != "" {
			fmt.Println("got error from update: ", err.Error())
		}
	}
	return err
}

func (t *tigrisDB) Insert(ctx context.Context, collection string, key string, values map[string][]byte) error {
	doc := fmt.Sprintf(`{ "Key": "%s"`, key)
	for fieldName, fieldValue := range values {
		doc = doc + fmt.Sprintf(`, "%s": "%s"`, fieldName, fieldValue)
	}
	doc = doc + " }"

	_, err := t.db.Insert(ctx, collection, []driver.Document{driver.Document(doc)})
	if err != nil {
		if os.Getenv("TIGRIS_PRINT_ERRORS") != "" {
			fmt.Println("got error from insert: ", err.Error())
		}
	}
	return err
}

func (t *tigrisDB) Delete(ctx context.Context, collection string, key string) error {
	filter := fmt.Sprintf(`{ "Key": "%s"`, key)
	_, err := t.db.Delete(ctx, collection, driver.Filter(filter))
	if err != nil {
		if os.Getenv("TIGRIS_PRINT_ERRORS") != "" {
			fmt.Println("got error from delete: ", err.Error())
		}
	}
	return err
}

func (c tigrisCreator) Create(p *properties.Properties) (ycsb.DB, error) {
	ctx := context.Background()
	dbName := p.GetString(tigrisDBName, "ycsb")
	collName := p.GetString(tigrisCollName, prop.TableNameDefault)
	host := p.GetString(tigrisHost, "localhost")
	port := p.GetInt(tigrisPort, 8081)
	clientId := os.Getenv("TIGRIS_CLIENT_ID")
	clientSecret := os.Getenv("TIGRIS_CLIENT_SECRET")
	token := os.Getenv("TIGRIS_TOKEN")
	url := fmt.Sprintf("%s:%d", host, port)
	fieldCount = p.GetInt64(tigrisFieldCount, prop.FieldCountDefault)
	conf := config.Driver{
		URL:          url,
		ClientID:     clientId,
		ClientSecret: clientSecret,
		Token:        token,
	}

	proto := p.GetString(tigrisProtocol, "grpc")
	if proto == "" {
		proto = os.Getenv("TIGRIS_PROTOCOL")
	}
	if strings.ToLower(proto) == "grpc" {
		driver.DefaultProtocol = driver.GRPC
	} else if strings.ToLower(proto) == "http" {
		driver.DefaultProtocol = driver.HTTP
	} else if strings.ToLower(proto) == "https" {
		driver.DefaultProtocol = driver.HTTP
		conf.TLS = &tls.Config{}
	}

	client, err := driver.NewDriver(ctx, &conf)
	if err != nil {
		if os.Getenv("TIGRIS_PRINT_ERRORS") != "" {
			fmt.Println("got error from creating tigris client: ", err.Error())
		}
	}

	err = client.CreateDatabase(ctx, dbName)
	if err != nil {
		if os.Getenv("TIGRIS_PRINT_ERRORS") != "" {
			fmt.Println("got error from creating database: ", err.Error())
		}
	}

	db := client.UseDatabase(dbName)

	schemaHead := fmt.Sprintf(`{ "title": "%s", "properties": { "Key": { "type": "string"}`, collName)
	if err != nil {
		if os.Getenv("TIGRIS_PRINT_ERRORS") != "" {
			fmt.Println("got error while assembling the head the the schema: ", err.Error())
		}
	}

	schema := schemaHead
	for i := int64(0); i < fieldCount; i++ {
		fieldSchema := fmt.Sprintf(`, "field%d": { "type": "string" }`, i)
		schema = schema + fieldSchema
	}

	schema = schema + `}, "primary_key": ["Key"] }`

	err = db.CreateOrUpdateCollection(ctx, collName, driver.Schema(schema))
	if err != nil {
		if os.Getenv("TIGRIS_PRINT_ERRORS") != "" {
			fmt.Println("got error while creating collection: ", err.Error())
		}
	}

	t := &tigrisDB{
		db: db,
	}
	return t, nil
}

func (t *tigrisDB) Close() error {
	return nil
}

func (t *tigrisDB) ToSqlDB() *sql.DB {
	return nil
}

func init() {
	ycsb.RegisterDBCreator("tigris", tigrisCreator{})
}
