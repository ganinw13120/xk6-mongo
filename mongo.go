package xk6_mongo

import (
	"context"
	"fmt"
	"github.com/ganinw13120/xk6-mongo/database"
	"github.com/ganinw13120/xk6-mongo/mongo"
	k6modules "go.k6.io/k6/js/modules"
	"go.mongodb.org/mongo-driver/bson"
)

func init() {
	k6modules.Register("k6/x/mongo", new(Mongo))
}

type Mongo struct{}

func main() {
	client, err := database.NewMongoDBConnection(context.TODO(), "mongodb+srv://spider-data-history:wYxoSKhlt6Np7060@spider-dev.673kr.mongodb.net/")
	if err != nil {
		panic(err)
	}
	db := database.NewMongoDB(client)
	fmt.Println("Connected")
	col := client.Database("history").Collection("history-account")
	var result interface{}

	err = db.FindOne(context.TODO(), col, &result, bson.E{"id", ""})
	fmt.Println(result)
	fmt.Println(err)
	//as := Mongo{}
	//as.NewClient("mongodb://gan:lHxWqJM30n3gthny@spider-dev.673kr.mongodb.net", "history", "history-post", "")
	//as.NewClient("mongodb+srv://spider-data-history:wYxoSKhlt6Np7060@spider-dev.673kr.mongodb.net/", "history", "history-post", "")
}

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
