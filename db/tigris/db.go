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
	tigrisDBName          = "tigris.dbname"
	tigrisHost            = "tigris.host"
	tigrisPort            = "tigris.port"
	tigrisProtocol        = "tigris.protocol"
	tigrisCollName        = "tigris.collection"
	tigrisIndexFieldCount = "tigris.indexfieldcount"
	tigrisIndexRead       = "tigris.indexread"
	pkFieldName           = "Key"
	skFieldName           = "secondaryKey"
)

type tigrisDB struct {
	db driver.Database
}

type tigrisCreator struct {
}

var fieldCount int64
var indexRead bool = false

func (t *tigrisDB) InitThread(ctx context.Context, _ int, _ int) context.Context {
	// TODO: multiple connection here, otherwise it will hit only one tigris app server
	return ctx
}

func (t *tigrisDB) CleanupThread(_ context.Context) {
}

func (t *tigrisDB) read(ctx context.Context, collection string, startKey string, scan bool, count int, fields []string) ([]map[string][]byte, error) {
	var doc driver.Document
	var res []map[string][]byte
	projection := make(map[string]bool)

	fieldName := pkFieldName
	if indexRead {
		fieldName = skFieldName
	}

	queryType := "$eq"
	if scan && indexRead {
		queryType = "$gte"
	}

	filter := map[string]interface{}{
		fieldName: map[string]interface{}{
			queryType: startKey,
		},
	}

	var included bool
	projection[pkFieldName] = true
	if indexRead {
		projection[skFieldName] = true
	}
	for i := int64(0); i < fieldCount; i++ {
		included = false
		currentFieldName := fmt.Sprintf("field%d", i)
		for _, field := range fields {
			if currentFieldName == field {
				included = true
			}
		}
		projection[currentFieldName] = included
	}

	filterJson, err := json.Marshal(filter)
	if err != nil {
		if os.Getenv("TIGRIS_PRINT_ERRORS") != "" {
			fmt.Println("got error while building the read filter: ", err.Error())
		}
		return nil, err
	}

	projectionJSON, err := json.Marshal(projection)
	if err != nil {
		if os.Getenv("TIGRIS_PRINT_ERRORS") != "" {
			fmt.Println("got error while building the read projection: ", err.Error())
		}
		return nil, err
	}
	it, err := t.db.Read(ctx, collection, filterJson, projectionJSON, &driver.ReadOptions{Limit: int64(count)})
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
	res, err := t.read(ctx, collection, key, false, 1, fields)
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
	return t.read(ctx, collection, startKey, true, count, fields)
}

func (t *tigrisDB) Update(ctx context.Context, collection string, key string, values map[string][]byte) error {
	fields := make(map[string]string)
	for fieldName, fieldValue := range values {
		fields[fieldName] = string(fieldValue)
	}
	update := map[string]interface{}{
		"$set": fields,
	}
	updateJson, err := json.Marshal(update)
	if err != nil {
		if os.Getenv("TIGRIS_PRINT_ERRORS") != "" {
			fmt.Println("got error while assembling the update json: ", err.Error())
		}
		return err
	}
	filter := map[string]string{
		pkFieldName: key,
	}
	filterJSON, err := json.Marshal(filter)
	if err != nil {
		if os.Getenv("TIGRIS_PRINT_ERRORS") != "" {
			fmt.Println("got error while building the read projection: ", err.Error())
		}
		return err
	}

	_, err = t.db.Update(ctx, collection, driver.Filter(filterJSON), driver.Update(updateJson))
	if err != nil {
		if os.Getenv("TIGRIS_PRINT_ERRORS") != "" {
			fmt.Println("got error from update: ", err.Error())
		}
	}
	return err
}

func (t *tigrisDB) Insert(ctx context.Context, collection string, key string, values map[string][]byte) error {
	doc := make(map[string]string)
	doc[pkFieldName] = key
	for fieldName, fieldValue := range values {
		doc[fieldName] = string(fieldValue)
	}
	if indexRead {
		doc[skFieldName] = key
	}

	docJson, err := json.Marshal(doc)
	if err != nil {
		if os.Getenv("TIGRIS_PRINT_ERRORS") != "" {
			fmt.Println("got error while assembling the update json: ", err.Error())
		}
		return err
	}

	_, err = t.db.Replace(ctx, collection, []driver.Document{driver.Document(docJson)})
	if err != nil {
		if os.Getenv("TIGRIS_PRINT_ERRORS") != "" {
			fmt.Println("got error from insert: ", err.Error())
		}
	}
	return err
}

func (t *tigrisDB) Delete(ctx context.Context, collection string, key string) error {
	filter := map[string]string{
		pkFieldName: key,
	}
	filterJSON, err := json.Marshal(filter)
	if err != nil {
		if os.Getenv("TIGRIS_PRINT_ERRORS") != "" {
			fmt.Println("got error while building the read projection: ", err.Error())
		}
		return err
	}
	_, err = t.db.Delete(ctx, collection, driver.Filter(filterJSON))
	if err != nil {
		if os.Getenv("TIGRIS_PRINT_ERRORS") != "" {
			fmt.Println("got error from delete: ", err.Error())
		}
	}
	return err
}

type Field struct {
	FieldType string `json:"type"`
	Index     bool   `json:"index"`
}

type Schema struct {
	Title      string           `json:"title"`
	Properties map[string]Field `json:"properties"`
	PrimaryKey []string         `json:"primary_key"`
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
	fieldCount = p.GetInt64(prop.FieldCount, prop.FieldCountDefault)
	indexfieldCount := p.GetInt64(tigrisIndexFieldCount, 0)
	indexRead = p.GetBool(tigrisIndexRead, false)
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

	_, err = client.CreateProject(ctx, dbName)
	if err != nil {
		if os.Getenv("TIGRIS_PRINT_ERRORS") != "" {
			fmt.Println("got error from creating database: ", err.Error())
		}
	}

	db := client.UseDatabase(dbName)

	if err != nil {
		if os.Getenv("TIGRIS_PRINT_ERRORS") != "" {
			fmt.Println("got error while assembling the head the the schema: ", err.Error())
		}
	}

	schema := Schema{
		Title: collName,
		Properties: map[string]Field{
			pkFieldName: {
				FieldType: "string",
				Index:     false,
			},
		},
		PrimaryKey: []string{pkFieldName},
	}

	indexedFields := int64(0)
	for i := int64(0); i < fieldCount; i++ {
		index := false
		if indexedFields <= indexfieldCount {
			index = true
			indexedFields += 1
		}

		name := fmt.Sprintf("field%d", i)
		schema.Properties[name] = Field{
			FieldType: "string",
			Index:     index,
		}
	}

	// To force a read from the secondary index
	// We create another field that is exactly the same values as "Key"
	// But we can filter by this field when reading to use the secondary index
	if indexRead {
		schema.Properties[skFieldName] = Field{
			FieldType: "string",
			Index:     true,
		}
	}

	raw, err := json.Marshal(schema)
	if err != nil {
		if os.Getenv("TIGRIS_PRINT_ERRORS") != "" {
			fmt.Println("got error while assembling the head the the schema: ", err.Error())
		}
		return nil, err
	}

	err = db.CreateOrUpdateCollection(ctx, collName, driver.Schema(string(raw)))
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
