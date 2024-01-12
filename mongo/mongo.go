package mongo

import (
	"context"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
)

func NewMongoDBConnection(ctx context.Context, uri string) (*mongo.Client, error) {
	client, err := mongo.Connect(ctx, options.Client().ApplyURI(uri))
	if err != nil {
		return nil, err
	}
	return client, nil
}

type MongoDB interface {
	FindOne(filter interface{}, opts ...*options.FindOneOptions) (interface{}, error)
	Find(filter interface{}, opts ...*options.FindOptions) (interface{}, error)
	InsertOne(document interface{}, opts ...*options.InsertOneOptions) (*primitive.ObjectID, error)
	InsertMany(documents []interface{}, opts ...*options.InsertManyOptions) ([]primitive.ObjectID, error)
	UpdateOne(filter interface{}, update interface{}, opts ...*options.UpdateOptions) (bool, error)
	UpdateMany(filter interface{}, update interface{}, opts ...*options.UpdateOptions) (int64, error)
	DeleteOne(filter interface{}, opts ...*options.DeleteOptions) (bool, error)
	Aggregate(pipeline interface{}, opts ...*options.AggregateOptions) (interface{}, error)
	Ping(rp *readpref.ReadPref) error
}

type mongodb struct {
	MongoClient *mongo.Client
	Collection  MongoCollection
}

func NewMongoDB(mongoClient *mongo.Client, collection MongoCollection) *mongodb {
	return &mongodb{
		mongoClient,
		collection,
	}
}

func NewCollection(mongoClient *mongo.Client, database, collection string) MongoCollection {
	return mongoClient.Database(database).Collection(collection)
}

func (m *mongodb) FindOne(filter interface{}, opts ...*options.FindOneOptions) (interface{}, error) {
	ctx := context.TODO()
	var result interface{}
	r := m.Collection.FindOne(ctx, filter, opts...)
	if r.Err() != nil {
		return nil, r.Err()
	}
	err := r.Decode(result)
	if err != nil {
		return nil, err
	}
	return result, nil
}

func (m *mongodb) Find(filter interface{}, opts ...*options.FindOptions) (interface{}, error) {
	ctx := context.TODO()
	var result interface{}
	cursor, err := m.Collection.Find(ctx, filter, opts...)
	if err != nil {
		return nil, err
	}
	err = cursor.All(ctx, result)
	if err != nil {
		return nil, err
	}
	return result, err
}

func (m *mongodb) InsertOne(document interface{}, opts ...*options.InsertOneOptions) (*primitive.ObjectID, error) {
	ctx := context.TODO()
	result, err := m.Collection.InsertOne(ctx, document, opts...)
	if err != nil {
		return nil, err
	}
	id := result.InsertedID.(primitive.ObjectID)
	return &id, nil
}

func (m *mongodb) InsertMany(documents []interface{}, opts ...*options.InsertManyOptions) ([]primitive.ObjectID, error) {
	var insertedIds []primitive.ObjectID
	ctx := context.TODO()
	result, err := m.Collection.InsertMany(ctx, documents)
	if err != nil {
		return nil, err
	}
	for _, ids := range result.InsertedIDs {
		insertedIds = append(insertedIds, ids.(primitive.ObjectID))
	}
	return insertedIds, nil
}

func (m *mongodb) UpdateOne(filter interface{}, update interface{}, opts ...*options.UpdateOptions) (bool, error) {
	ctx := context.TODO()
	result, err := m.Collection.UpdateOne(ctx, filter, update, opts...)
	if err != nil {
		return false, err
	}
	return result.ModifiedCount > 0, nil
}

func (m *mongodb) UpdateMany(filter interface{}, update interface{}, opts ...*options.UpdateOptions) (int64, error) {
	ctx := context.TODO()
	result, err := m.Collection.UpdateMany(ctx, filter, update, opts...)
	if err != nil {
		return 0, err
	}
	return result.ModifiedCount, nil

}

func (m *mongodb) DeleteOne(filter interface{}, opts ...*options.DeleteOptions) (bool, error) {
	ctx := context.TODO()
	result, err := m.Collection.DeleteOne(ctx, filter, opts...)
	if err != nil {
		return false, err
	}

	return result.DeletedCount > 0, nil
}

func (m *mongodb) Aggregate(pipeline interface{}, opts ...*options.AggregateOptions) interface{} {
	ctx := context.TODO()
	result := make([]interface{}, 10000)
	cursor, err := m.Collection.Aggregate(ctx, pipeline, opts...)
	if err != nil {
		return err
	}
	if err = cursor.All(ctx, &result); err != nil {
		return err
	}
	return result
}
func (adapter *mongodb) Ping() error {
	ctx := context.TODO()
	return adapter.MongoClient.Ping(ctx, nil)
}

type MongoCollection interface {
	// Clone creates a copy of the Collection configured with the given CollectionOptions.
	// The specified options are merged with the existing options on the collection, with the specified options taking
	// precedence.
	Clone(opts ...*options.CollectionOptions) (*mongo.Collection, error)
	// Name returns the name of the collection.
	Name() string
	// Database returns the Database that was used to create the Collection.
	Database() *mongo.Database
	// BulkWrite performs a bulk write operation (https://docs.mongodb.com/manual/core/bulk-write-operations/).
	//
	// The models parameter must be a slice of operations to be executed in this bulk write. It cannot be nil or empty.
	// All of the models must be non-nil. See the mongo.WriteModel documentation for a list of valid model types and
	// examples of how they should be used.
	//
	// The opts parameter can be used to specify options for the operation (see the options.BulkWriteOptions documentation.)
	BulkWrite(ctx context.Context, models []mongo.WriteModel, opts ...*options.BulkWriteOptions) (*mongo.BulkWriteResult, error)
	// InsertOne executes an insert command to insert a single document into the collection.
	//
	// The document parameter must be the document to be inserted. It cannot be nil. If the document does not have an _id
	// field when transformed into BSON, one will be added automatically to the marshalled document. The original document
	// will not be modified. The _id can be retrieved from the InsertedID field of the returned InsertOneResult.
	//
	// The opts parameter can be used to specify options for the operation (see the options.InsertOneOptions documentation.)
	//
	// For more information about the command, see https://docs.mongodb.com/manual/reference/command/insert/.
	InsertOne(ctx context.Context, document interface{}, opts ...*options.InsertOneOptions) (*mongo.InsertOneResult, error)
	// InsertMany executes an insert command to insert multiple documents into the collection. If write errors occur
	// during the operation (e.g. duplicate key error), this method returns a BulkWriteException error.
	//
	// The documents parameter must be a slice of documents to insert. The slice cannot be nil or empty. The elements must
	// all be non-nil. For any document that does not have an _id field when transformed into BSON, one will be added
	// automatically to the marshalled document. The original document will not be modified. The _id values for the inserted
	// documents can be retrieved from the InsertedIDs field of the returned InsertManyResult.
	//
	// The opts parameter can be used to specify options for the operation (see the options.InsertManyOptions documentation.)
	//
	// For more information about the command, see https://docs.mongodb.com/manual/reference/command/insert/.
	InsertMany(ctx context.Context, documents []interface{}, opts ...*options.InsertManyOptions) (*mongo.InsertManyResult, error)
	// DeleteOne executes a delete command to delete at most one document from the collection.
	//
	// The filter parameter must be a document containing query operators and can be used to select the document to be
	// deleted. It cannot be nil. If the filter does not match any documents, the operation will succeed and a DeleteResult
	// with a DeletedCount of 0 will be returned. If the filter matches multiple documents, one will be selected from the
	// matched set.
	//
	// The opts parameter can be used to specify options for the operation (see the options.DeleteOptions documentation).
	//
	// For more information about the command, see https://docs.mongodb.com/manual/reference/command/delete/.
	DeleteOne(ctx context.Context, filter interface{}, opts ...*options.DeleteOptions) (*mongo.DeleteResult, error)
	// DeleteMany executes a delete command to delete documents from the collection.
	//
	// The filter parameter must be a document containing query operators and can be used to select the documents to
	// be deleted. It cannot be nil. An empty document (e.g. bson.D{}) should be used to delete all documents in the
	// collection. If the filter does not match any documents, the operation will succeed and a DeleteResult with a
	// DeletedCount of 0 will be returned.
	//
	// The opts parameter can be used to specify options for the operation (see the options.DeleteOptions documentation).
	//
	// For more information about the command, see https://docs.mongodb.com/manual/reference/command/delete/.
	DeleteMany(ctx context.Context, filter interface{}, opts ...*options.DeleteOptions) (*mongo.DeleteResult, error)
	// UpdateByID executes an update command to update the document whose _id value matches the provided ID in the collection.
	// This is equivalent to running UpdateOne(ctx, bson.D{{"_id", id}}, update, opts...).
	//
	// The id parameter is the _id of the document to be updated. It cannot be nil. If the ID does not match any documents,
	// the operation will succeed and an UpdateResult with a MatchedCount of 0 will be returned.
	//
	// The update parameter must be a document containing update operators
	// (https://docs.mongodb.com/manual/reference/operator/update/) and can be used to specify the modifications to be
	// made to the selected document. It cannot be nil or empty.
	//
	// The opts parameter can be used to specify options for the operation (see the options.UpdateOptions documentation).
	//
	// For more information about the command, see https://docs.mongodb.com/manual/reference/command/update/.
	UpdateByID(ctx context.Context, id interface{}, update interface{}, opts ...*options.UpdateOptions) (*mongo.UpdateResult, error)
	// UpdateOne executes an update command to update at most one document in the collection.
	//
	// The filter parameter must be a document containing query operators and can be used to select the document to be
	// updated. It cannot be nil. If the filter does not match any documents, the operation will succeed and an UpdateResult
	// with a MatchedCount of 0 will be returned. If the filter matches multiple documents, one will be selected from the
	// matched set and MatchedCount will equal 1.
	//
	// The update parameter must be a document containing update operators
	// (https://docs.mongodb.com/manual/reference/operator/update/) and can be used to specify the modifications to be
	// made to the selected document. It cannot be nil or empty.
	//
	// The opts parameter can be used to specify options for the operation (see the options.UpdateOptions documentation).
	//
	// For more information about the command, see https://docs.mongodb.com/manual/reference/command/update/.
	UpdateOne(ctx context.Context, filter interface{}, update interface{}, opts ...*options.UpdateOptions) (*mongo.UpdateResult, error)
	// UpdateMany executes an update command to update documents in the collection.
	//
	// The filter parameter must be a document containing query operators and can be used to select the documents to be
	// updated. It cannot be nil. If the filter does not match any documents, the operation will succeed and an UpdateResult
	// with a MatchedCount of 0 will be returned.
	//
	// The update parameter must be a document containing update operators
	// (https://docs.mongodb.com/manual/reference/operator/update/) and can be used to specify the modifications to be made
	// to the selected documents. It cannot be nil or empty.
	//
	// The opts parameter can be used to specify options for the operation (see the options.UpdateOptions documentation).
	//
	// For more information about the command, see https://docs.mongodb.com/manual/reference/command/update/.
	UpdateMany(ctx context.Context, filter interface{}, update interface{}, opts ...*options.UpdateOptions) (*mongo.UpdateResult, error)
	// ReplaceOne executes an update command to replace at most one document in the collection.
	//
	// The filter parameter must be a document containing query operators and can be used to select the document to be
	// replaced. It cannot be nil. If the filter does not match any documents, the operation will succeed and an
	// UpdateResult with a MatchedCount of 0 will be returned. If the filter matches multiple documents, one will be
	// selected from the matched set and MatchedCount will equal 1.
	//
	// The replacement parameter must be a document that will be used to replace the selected document. It cannot be nil
	// and cannot contain any update operators (https://docs.mongodb.com/manual/reference/operator/update/).
	//
	// The opts parameter can be used to specify options for the operation (see the options.ReplaceOptions documentation).
	//
	// For more information about the command, see https://docs.mongodb.com/manual/reference/command/update/.
	ReplaceOne(ctx context.Context, filter interface{}, replacement interface{}, opts ...*options.ReplaceOptions) (*mongo.UpdateResult, error)
	// Aggregate executes an aggregate command against the collection and returns a cursor over the resulting documents.
	//
	// The pipeline parameter must be an array of documents, each representing an aggregation stage. The pipeline cannot
	// be nil but can be empty. The stage documents must all be non-nil. For a pipeline of bson.D documents, the
	// mongo.Pipeline type can be used. See
	// https://docs.mongodb.com/manual/reference/operator/aggregation-pipeline/#db-collection-aggregate-stages for a list of
	// valid stages in aggregations.
	//
	// The opts parameter can be used to specify options for the operation (see the options.AggregateOptions documentation.)
	//
	// For more information about the command, see https://docs.mongodb.com/manual/reference/command/aggregate/.
	Aggregate(ctx context.Context, pipeline interface{}, opts ...*options.AggregateOptions) (*mongo.Cursor, error)
	// CountDocuments returns the number of documents in the collection. For a fast count of the documents in the
	// collection, see the EstimatedDocumentCount method.
	//
	// The filter parameter must be a document and can be used to select which documents contribute to the count. It
	// cannot be nil. An empty document (e.g. bson.D{}) should be used to count all documents in the collection. This will
	// result in a full collection scan.
	//
	// The opts parameter can be used to specify options for the operation (see the options.CountOptions documentation).
	CountDocuments(ctx context.Context, filter interface{}, opts ...*options.CountOptions) (int64, error)
	// EstimatedDocumentCount executes a count command and returns an estimate of the number of documents in the collection
	// using collection metadata.
	//
	// The opts parameter can be used to specify options for the operation (see the options.EstimatedDocumentCountOptions
	// documentation).
	//
	// For more information about the command, see https://docs.mongodb.com/manual/reference/command/count/.
	EstimatedDocumentCount(ctx context.Context, opts ...*options.EstimatedDocumentCountOptions) (int64, error)
	// Distinct executes a distinct command to find the unique values for a specified field in the collection.
	//
	// The fieldName parameter specifies the field name for which distinct values should be returned.
	//
	// The filter parameter must be a document containing query operators and can be used to select which documents are
	// considered. It cannot be nil. An empty document (e.g. bson.D{}) should be used to select all documents.
	//
	// The opts parameter can be used to specify options for the operation (see the options.DistinctOptions documentation).
	//
	// For more information about the command, see https://docs.mongodb.com/manual/reference/command/distinct/.
	Distinct(ctx context.Context, fieldName string, filter interface{}, opts ...*options.DistinctOptions) ([]interface{}, error)
	// Find executes a find command and returns a Cursor over the matching documents in the collection.
	//
	// The filter parameter must be a document containing query operators and can be used to select which documents are
	// included in the result. It cannot be nil. An empty document (e.g. bson.D{}) should be used to include all documents.
	//
	// The opts parameter can be used to specify options for the operation (see the options.FindOptions documentation).
	//
	// For more information about the command, see https://docs.mongodb.com/manual/reference/command/find/.
	Find(ctx context.Context, filter interface{}, opts ...*options.FindOptions) (cur *mongo.Cursor, err error)
	// FindOne executes a find command and returns a SingleResult for one document in the collection.
	//
	// The filter parameter must be a document containing query operators and can be used to select the document to be
	// returned. It cannot be nil. If the filter does not match any documents, a SingleResult with an error set to
	// ErrNoDocuments will be returned. If the filter matches multiple documents, one will be selected from the matched set.
	//
	// The opts parameter can be used to specify options for this operation (see the options.FindOneOptions documentation).
	//
	// For more information about the command, see https://docs.mongodb.com/manual/reference/command/find/.
	FindOne(ctx context.Context, filter interface{}, opts ...*options.FindOneOptions) *mongo.SingleResult
	// FindOneAndDelete executes a findAndModify command to delete at most one document in the collection. and returns the
	// document as it appeared before deletion.
	//
	// The filter parameter must be a document containing query operators and can be used to select the document to be
	// deleted. It cannot be nil. If the filter does not match any documents, a SingleResult with an error set to
	// ErrNoDocuments wil be returned. If the filter matches multiple documents, one will be selected from the matched set.
	//
	// The opts parameter can be used to specify options for the operation (see the options.FindOneAndDeleteOptions
	// documentation).
	//
	// For more information about the command, see https://docs.mongodb.com/manual/reference/command/findAndModify/.
	FindOneAndDelete(ctx context.Context, filter interface{}, opts ...*options.FindOneAndDeleteOptions) *mongo.SingleResult
	// FindOneAndReplace executes a findAndModify command to replace at most one document in the collection
	// and returns the document as it appeared before replacement.
	//
	// The filter parameter must be a document containing query operators and can be used to select the document to be
	// replaced. It cannot be nil. If the filter does not match any documents, a SingleResult with an error set to
	// ErrNoDocuments wil be returned. If the filter matches multiple documents, one will be selected from the matched set.
	//
	// The replacement parameter must be a document that will be used to replace the selected document. It cannot be nil
	// and cannot contain any update operators (https://docs.mongodb.com/manual/reference/operator/update/).
	//
	// The opts parameter can be used to specify options for the operation (see the options.FindOneAndReplaceOptions
	// documentation).
	//
	// For more information about the command, see https://docs.mongodb.com/manual/reference/command/findAndModify/.
	FindOneAndReplace(ctx context.Context, filter interface{}, replacement interface{}, opts ...*options.FindOneAndReplaceOptions) *mongo.SingleResult
	// FindOneAndUpdate executes a findAndModify command to update at most one document in the collection and returns the
	// document as it appeared before updating.
	//
	// The filter parameter must be a document containing query operators and can be used to select the document to be
	// updated. It cannot be nil. If the filter does not match any documents, a SingleResult with an error set to
	// ErrNoDocuments wil be returned. If the filter matches multiple documents, one will be selected from the matched set.
	//
	// The update parameter must be a document containing update operators
	// (https://docs.mongodb.com/manual/reference/operator/update/) and can be used to specify the modifications to be made
	// to the selected document. It cannot be nil or empty.
	//
	// The opts parameter can be used to specify options for the operation (see the options.FindOneAndUpdateOptions
	// documentation).
	//
	// For more information about the command, see https://docs.mongodb.com/manual/reference/command/findAndModify/.
	FindOneAndUpdate(ctx context.Context, filter interface{}, update interface{}, opts ...*options.FindOneAndUpdateOptions) *mongo.SingleResult
	// Watch returns a change stream for all changes on the corresponding collection. See
	// https://docs.mongodb.com/manual/changeStreams/ for more information about change streams.
	//
	// The Collection must be configured with read concern majority or no read concern for a change stream to be created
	// successfully.
	//
	// The pipeline parameter must be an array of documents, each representing a pipeline stage. The pipeline cannot be
	// nil but can be empty. The stage documents must all be non-nil. See https://docs.mongodb.com/manual/changeStreams/ for
	// a list of pipeline stages that can be used with change streams. For a pipeline of bson.D documents, the
	// mongo.Pipeline{} type can be used.
	//
	// The opts parameter can be used to specify options for change stream creation (see the options.ChangeStreamOptions
	// documentation).
	Watch(ctx context.Context, pipeline interface{}, opts ...*options.ChangeStreamOptions) (*mongo.ChangeStream, error)
	// Indexes returns an IndexView instance that can be used to perform operations on the indexes for the collection.
	Indexes() mongo.IndexView
	// Drop drops the collection on the server. This method ignores "namespace not found" errors so it is safe to drop
	// a collection that does not exist on the server.
	Drop(ctx context.Context) error
}
