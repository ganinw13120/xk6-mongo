package xk6_mongo

import (
	"context"
	"fmt"
	"github.com/ganinw13120/xk6-mongo/mongo"
	k6modules "go.k6.io/k6/js/modules"
)

func init() {
	k6modules.Register("k6/x/mongo", new(Mongo))
}

type Mongo struct{}

func (*Mongo) NewClient(uri, database, collection string, pipeline interface{}) interface{} {
	client, err := mongo.NewMongoDBConnection(context.TODO(), uri)
	if err != nil {
		fmt.Println(err)
		return err
	}

	col := client.Database(database).Collection(collection)
	db := mongo.NewMongoDB(client, col)

	return db
}
