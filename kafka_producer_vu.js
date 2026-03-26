import { check } from 'k6';
import { Writer, SchemaRegistry, SCHEMA_TYPE_JSON } from 'k6/x/kafka';
import { sleep } from 'k6';
import exec from 'k6/execution';

const brokers = ['localhost:9092'];
const topic = 'VTB_topic_1';
const PACING_CYCLE = 1;

const writer = new Writer({
    brokers: brokers,
    topic: topic,
    acks: 1,
});

const schemaRegistry = new SchemaRegistry();

export const options = {
    discardResponseBodies: false,
    scenarios: {
        scenario: {
            executor: 'ramping-vus',
            startVUs: 0,
            stages: [
                { duration: '0s', target: 5 },
                { duration: '5m', target: 5 },   // 5 RPS в течение 5 минут
                { duration: '0s', target: 10 },
                { duration: '5m', target: 10 },  // 10 RPS в течение 5 минут
            ],
        },
    }
};    
export default function () {
    const start = Date.now();
    const partition = exec.scenario.iterationInTest % 2;
    //const partition = (__VU + __ITER) % 2;

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

    const elapsed = (Date.now() - start) / 1000;
    const pause = PACING_CYCLE - elapsed;
    if (pause > 0) sleep(pause);
}

export function teardown() {
    writer.close();
}