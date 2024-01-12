import xk6_mongo from 'k6/x/mongo';

const client = xk6_mongo.newClient('mongodb://localhost:27017/', "db","collection");

export const options = {
    vus: 10,
    duration: '5m',
}

export default ()=> {
    const result = client.aggregate([
        {
            $match: {
                id: "",
            },
        },
        {
            $project: {
                meta: 1,
                collected_at: 1
            },
        },
        {
            $sort: {
                collected_at: -1,
            },
        },
    ])
}