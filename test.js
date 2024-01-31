import xk6_mongo from 'k6/x/mongo';

const client = xk6_mongo.newClient('mongodb://localhost:27017/', "db", "collection");

export const options = {
    vus: 10,
    duration: '5m',
}

export default () => {
    const result = client.aggregate([
        {
            $match: {
                name: "John",
            },
        },
        {
            $project: {
                name: 1,
                address: 1,
                created_at: 1
            },
        },
        {
            $sort: {
                created_at: -1,
            },
        },
    ])

    console.log(result)

    check(result, {
        'Has result': (r) => r.length > 0,
    });
}