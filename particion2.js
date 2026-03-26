import { check } from 'k6';
import { Writer, SchemaRegistry, SCHEMA_TYPE_JSON } from 'k6/x/kafka';
import { sleep } from 'k6';

const brokers = ['localhost:9092'];
const topic = 'VTB_topic_1';
const PACING_CYCLE = 2;
const schemaRegistry = new SchemaRegistry();

const writer = new Writer({
    brokers: brokers,
    topic: topic,
    acks: 1,
});

export const options = {
    discardResponseBodies: false,
    scenarios: {
        // Первый этап: 5 минут, 10 пользователей
        stage1_partition0: {
            executor: 'shared-iterations',
            vus: 5,
            iterations: 750,
            maxDuration: '5m',
            exec: 'writeToPartition0',
        },
        stage1_partition1: {
            executor: 'shared-iterations',
            vus: 5,
            iterations: 750,
            maxDuration: '5m',
            exec: 'writeToPartition1',
            startTime: '0s',
        },
        // Второй этап: 5 минут, 20 пользователей
        stage2_partition0: {
            executor: 'shared-iterations',
            vus: 10,
            iterations: 1500,
            maxDuration: '5m',
            exec: 'writeToPartition0',
            startTime: '5m',
        },
        stage2_partition1: {
            executor: 'shared-iterations',
            vus: 10,
            iterations: 1500,
            maxDuration: '5m',
            exec: 'writeToPartition1',
            startTime: '5m',
        },
    }
};

// Функция для записи в партицию 0
export function writeToPartition0() {
    const start = Date.now();
    const message = {
        partition: 0,
        value: schemaRegistry.serialize({
            data: {
                id: `${start}`,
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

// Функция для записи в партицию 1
export function writeToPartition1() {
    const start = Date.now();
    const message = {
        partition: 1,
        value: schemaRegistry.serialize({
            data: {
                id: `${start}`,
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