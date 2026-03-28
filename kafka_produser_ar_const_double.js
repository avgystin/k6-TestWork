import { check } from 'k6';
import { Writer, SchemaRegistry, SCHEMA_TYPE_JSON } from 'k6/x/kafka';
import exec from 'k6/execution';

const writer = new Writer({
    brokers: ['localhost:9092'],
    topic: 'VTB_topic_1',
    acks: 1,
});

const schemaRegistry = new SchemaRegistry();

export const options = {
    discardResponseBodies: false,
    scenarios: {
        scenario0: {
            executor: 'constant-arrival-rate',
            duration: '10m',
            rate: 5,                      
            timeUnit: '2s',                    
            preAllocatedVUs: 10,               
            maxVUs: 10,
            exec: 'partition0',                      
        },
        scenario0_1: {
            executor: 'constant-arrival-rate',
            duration: '10m',
            rate: 5,                      
            timeUnit: '2s',                    
            preAllocatedVUs: 10,               
            maxVUs: 10,
            exec: 'partition1',                      
        },
        scenario1: {
            executor: 'constant-arrival-rate',
            duration: '5m',
            rate: 5,                      
            timeUnit: '2s',                    
            preAllocatedVUs: 10,               
            maxVUs: 10,
            exec: 'partition0',
            startTime: '5m',                      
        },
        scenario1_1: {
            executor: 'constant-arrival-rate',
            duration: '5m',
            rate: 5,                      
            timeUnit: '2s',                    
            preAllocatedVUs: 10,               
            maxVUs: 10,
            exec: 'partition1',
            startTime: '5m',                      
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