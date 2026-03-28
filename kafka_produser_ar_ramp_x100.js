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
            startRate: 500,                      
            timeUnit: '1s',                    
            preAllocatedVUs: 10,               
            maxVUs: 20,                       
            stages: [
                { duration: '5m', target: 500 },
                { duration: '0s', target: 1000 },
                { duration: '5m', target: 1000 },
            ],
        },
    }
};    

export default function () {
    const partition = exec.scenario.iterationInTest % 2;

    const message = {
        partition: partition,
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