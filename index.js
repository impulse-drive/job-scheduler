// Configuration
const NAME = process.env.NAME;
const QUEUE_URL = process.env.QUEUE_URL;
const STORAGE_URL = process.env.STORAGE_URL;
const RESOURCES = process.env.RESOURCES;
const TASK_PATH = process.env.TASK_PATH;

if(!/^pipeline\.[0-9a-zA-Z\-\_]+\.job-scheduler\.[0-9a-zA-Z\-\_]+$/.test(NAME)) {
    console.error(`NAME=${NAME}: bad name`);
    process.exit(1);
}

if(!QUEUE_URL) {
    console.error(`QUEUE_URL=${QUEUE_URL}`);
    process.exit(1);
}

if(!STORAGE_URL) {
    console.error(`STORAGE_URL=${STORAGE_URL}`);
    process.exit(1);
}

RESOURCES.split(':').map(x => {
    if(!/^pipeline\.[0-9a-zA-Z\-\_]+\.resource\.[0-9a-zA-Z\-\_]+$/.test(x)) {
        console.error(`RESOURCES=${RESOURCES}: bad format`);
        process.exit(1);
    }
});

if(!fs.lstatSync(TASK_PATH).isDirectory()) {
    console.error(`TASK_PATH=${TASK_PATH}: not a directory`);
    process.exit(1);
}

const [ , pipeline, , job ] = NAME.split('.');

// Connections
const k8s = new (require('kubernetes-client').Client)({ version: '1.13' });
const nats = require('nats').connect(QUEUE_URL, { json: true });

// Monkey patch nats to provide logging
nats._publish = nats.publish;
nats.publish = (topic, payload) => {
    console.log({[topic]: payload});
    nats._publish(topic, payload);
};

// Common functions
Object.fromEntries = l => l.reduce((a, [k,v]) => ({...a, [k]: v}), {});
Object.allValuesNotNull = l => Object.values(l).reduce((a, v) => a && (v!=null), true);

// Initial state
var resources = Object.fromEntries(RESOURCES.split(':').map(x => [x, null]));

const notify = async payload => {
    await Promise.all(Object.entries(resources).map(
        async ([res, { identifier, url }]) => nats.publish(res, {identifier, url, ...payload})
    ));
};


// Resource handle
const spawnJob = async (name) => {
    notify({started: name});
    const tasks = fs.readdirSync(TASK_PATH);
    console.log(tasks);
};

const resourceUpdated = res => async ({ identifier, url }) => {
    if(!resources[res] || resources[res].identifier != identifier) {
        resources[res] = { identifier, url };
        if(Object.allValuesNotNull(resources)) {
            await spawnJob(res);
        }
    }
};

// Subscriptions
Object.keys(resources).map(res => nats.subscribe(res, resourceUpdated(res)));

//nc.publish(`${PIPELINE}/job/${NAME}`, { status: 'started' });

/*
const main = async () => {
    const namespaces = await k8s.api.v1.namespaces.get()
    console.log(namespaces)
}

main().catch(err => {
    console.error(err);
    process.exit(1);
});
*/
