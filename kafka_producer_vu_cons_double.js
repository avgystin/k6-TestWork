import { check } from 'k6';
import { Writer, SchemaRegistry, SCHEMA_TYPE_JSON } from 'k6/x/kafka';
import { sleep } from 'k6';
import exec from 'k6/execution';

const brokers = ['localhost:9092'];
const topic = 'VTB_topic_1';
const PACING_CYCLE = 2;

const writer = new Writer({
    brokers: brokers,
    topic: topic,
    acks: 1,
});

const schemaRegistry = new SchemaRegistry();

export const options = {
    discardResponseBodies: false,
      scenarios: {
        contacts1: {
        executor: 'constant-vus',
        vus: 5,
        duration: '10m',
        exec: 'partition0',
        },
        contacts1_2: {
        executor: 'constant-vus',
        vus: 5,
        duration: '10m',
        exec: 'partition1',
        },

        contacts2: {
        executor: 'constant-vus',
        vus: 5,
        duration: '5m',
        exec: 'partition0',
        startTime: '5m',
        },
        contacts2_2: {
        executor: 'constant-vus',
        vus: 5,
        duration: '5m',
        exec: 'partition1',
        startTime: '5m',
        },
    }
};
   
export function partition0() {
    const start = Date.now();

    const message = {
        partition: 0,
        value: schemaRegistry.serialize({
            data: {
                id: `${__VU}${__ITER}${start}`,
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
export function partition1() {
    const start = Date.now();

    const message = {
        partition: 1,
        value: schemaRegistry.serialize({
            data: {
                id: `${__VU}${__ITER}${start}`,
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