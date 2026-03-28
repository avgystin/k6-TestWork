import { check } from 'k6';
import { Writer, SchemaRegistry, SCHEMA_TYPE_JSON } from 'k6/x/kafka';
import { sleep } from 'k6';

const PACING_CYCLE = 2;
const schemaRegistry = new SchemaRegistry();
const writer = new Writer({
    brokers: ['localhost:9092'],
    topic: 'VTB_topic_1',
    acks: 1,
});

export const options = {
    discardResponseBodies: false,
    scenarios: {
        stage1_partition0: {
            executor: 'shared-iterations',
            vus: 5,
            iterations: 1500,
            maxDuration: '10m',
            exec: 'writeToPartition0',
        },
        stage1_partition1: {
            executor: 'shared-iterations',
            vus: 5,
            iterations: 1500,
            maxDuration: '10m',
            exec: 'writeToPartition1',
        },

        stage2_partition0: {
            executor: 'shared-iterations',
            vus: 5,
            iterations: 750,
            maxDuration: '5m',
            exec: 'writeToPartition0',
            startTime: '5m',
        },
        stage2_partition1: {
            executor: 'shared-iterations',
            vus: 5,
            iterations: 750,
            maxDuration: '5m',
            exec: 'writeToPartition1',
            startTime: '5m',
        },
    }
};

export function writeToPartition0() {
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
        'message sent to partition 0': (ans) => ans === undefined,
    });

    const elapsed = (Date.now() - start) / 1000;
    const pause = PACING_CYCLE - elapsed;
    if (pause > 0) sleep(pause);
}

export function writeToPartition1() {
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
        'message sent to partition 1': (ans) => ans === undefined,
    });

    const elapsed = (Date.now() - start) / 1000;
    const pause = PACING_CYCLE - elapsed;
    if (pause > 0) sleep(pause);
}

export function teardown() {
    writer.close();
}