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
        scenario: {
            executor: 'ramping-arrival-rate',
            startRate: 5,                      
            timeUnit: '1s',                    
            preAllocatedVUs: 10,               
            maxVUs: 20,                       
            stages: [
                { duration: '5m', target: 5 },
                { duration: '0s', target: 10 },
                { duration: '5m', target: 10 },
            ],
        },
    }
};    

export default function () {
    const iteration = exec.scenario.iterationInTest;

    const message = {
        partition: iteration % 2,
        value: schemaRegistry.serialize({
            data: {
                id: `${iteration}`,
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