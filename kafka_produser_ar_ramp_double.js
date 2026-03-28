import { check } from 'k6';
import { Writer, SchemaRegistry, SCHEMA_TYPE_JSON } from 'k6/x/kafka';
import exec from 'k6/execution';

const brokers = ['localhost:9092'];
const topic = 'VTB_topic_1';

const writer = new Writer({
    brokers: brokers,
    topic: topic,
    acks: 1,
});

const schemaRegistry = new SchemaRegistry();

export const options = {
    discardResponseBodies: false,
    scenarios: {
        scenario0: {
            executor: 'ramping-arrival-rate',
            startRate: 5,                      
            timeUnit: '2s',                    
            preAllocatedVUs: 10,               
            maxVUs: 20,
            exec: 'partition0',                       
            stages: [
                { duration: '5m', target: 5 },   // 5 минут держим 5 RPS
                { duration: '0s', target: 10 },
                { duration: '5m', target: 10 },  // Следующие 5 минут 10 RPS
            ],
        },
        scenario1: {
            executor: 'ramping-arrival-rate',
            startRate: 5,                      
            timeUnit: '2s',                    
            preAllocatedVUs: 10,               
            maxVUs: 20,
            exec: 'partition1',                       
            stages: [
                { duration: '5m', target: 5 },   // 5 минут держим 5 RPS
                { duration: '0s', target: 10 },
                { duration: '5m', target: 10 },  // Следующие 5 минут 10 RPS
            ],
        },
    }
};    

export function partition0() {

    const message = {
        partition: 0,
        value: schemaRegistry.serialize({
            data: {
                id: `${__VU}${__ITER}`,
            },
            schemaType: SCHEMA_TYPE_JSON,
        }),
    };

    const answer = writer.produce({ messages: [message] });
    
    check(answer, {
        'message sent': (ans) => ans === undefined,
    });
}
export function partition1() {

    const message = {
        partition: 1,
        value: schemaRegistry.serialize({
            data: {
                id: `${__VU}${__ITER}`,
            },
            schemaType: SCHEMA_TYPE_JSON,
        }),
    };

    const answer = writer.produce({ messages: [message] });
    
    check(answer, {
        'message sent': (ans) => ans === undefined,
    });
}
export function teardown() {
    writer.close();
}