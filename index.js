const fs = require('fs');
const path = require('path');
const { promisify } = require('util');

// Configuration
const NAME = process.env.NAME;
const QUEUE_URL = process.env.QUEUE_URL;
const STORAGE_URL = process.env.STORAGE_URL;
const RESOURCES = process.env.RESOURCES;
const TASK_PATH = process.env.TASK_PATH;
const REDIS_HOST = process.env.REDIS_HOST;
const REDIS_PORT = process.env.REDIS_PORT;

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

if(!REDIS_HOST) {
    console.error(`REDIS_HOST=${REDIS_HOST}`);
    process.exit(1);
}

if(!REDIS_PORT) {
    console.log(`REDIS_PORT=${REDIS_PORT}`);
    process.exit(1);
}

RESOURCES.split(':').map(x => {
    if(!/^pipeline\.[0-9a-zA-Z\-\_]+\.resource\.[0-9a-zA-Z\-\_]+$/.test(x)) {
        console.error(`RESOURCES=${RESOURCES}: bad format`);
        process.exit(1);
    }
});
/*
if(!fs.lstatSync(TASK_PATH).isFile()) {
    console.error(`TASK_PATH=${TASK_PATH}: not a file`);
    process.exit(1);
}
*/

// Connections
const nats = require('nats').connect(QUEUE_URL, { json: true });
nats._publish = nats.publish;
nats.publish = (topic, payload) => {
    console.log({[topic]: payload});
    nats._publish(topic, payload);
};

const redis = require('async-redis').createClient({ host: REDIS_HOST, port: REDIS_PORT });

redis.on('error', error => {
    console.error(error);
    process.exit(1);
});

// Common functions
Object.fromEntries = l => l.reduce((a, [k,v]) => ({...a, [k]: v}), {});
Object.allValuesNotNull = l => Object.values(l).reduce((a, v) => a && (v!=null), true);

// Initial state
const [ , pipeline, , job ] = NAME.split('.');
const tasks = JSON.parse(fs.readFileSync(TASK_PATH));
const jobName = `pipeline.${pipeline}.job.${job}`;
const resources = RESOURCES.split(':');


// State lookup
const getCommits = async () => {
    return Object.fromEntries(await Promise.all(resources.map(async res => {
        return await redis.get(`${jobName}.${res}.commit`).then(id => [res, id])
    })));
};

const getUrls = async () => {
    return Object.fromEntries(await Promise.all(resources.map(async res => {
        return await redis.get(`${jobName}.${res}.url`).then(id => [res, id])
    })));
};


const setCommit = async (res, id) => {
    await redis.set(`${jobName}.${res}.commit`,  id);
};

const setUrl = async (res, url) => {
    await redis.set(`${jobName}.${res}.url`, url);
};

const getBuild = async () => {
    return await redis.incr(jobName);
};

// Resource handle
const spawnJob = async (build) => {
    const urls = await getUrls();
    const inputUrls = Object.fromEntries(Object.entries(urls).map(([k,v]) => [k.split('.').slice(-1)[0], v]));
    for(i = 0 ; i < tasks.length ; ++i) {
        console.log(`spawning task: ${tasks[i]}`);
        const task = tasks[i];
        const taskName = `${jobName}.task.${task.name}`;
        const outputUrl = `${STORAGE_URL}/${taskName.replace(/\./g, '/')}/${build}.tar.gz`;
        const promise = new Promise((resolve, reject) => {
            const subject = `${taskName}.start`;
            const data = { ...task, build, outputUrl, inputUrls };
            const options = { max: 1 };
            nats.request(subject, data, options, ({status}) => {
                if(status === 'failed') {
                    return reject(status);
                }
                return resolve(status);
            });
        });
        const status = await promise.catch(status => {
            nats.publish(jobName, { build, status: 'failed' });
            console.log(`task ${taskName} failed`);
            console.log(`job ${jboName} failed`);
            throw status;
        });
        console.log(`task ${taskName} succeeded`);
    }
    nats.publish(jobName, { build, status: 'succeeded' });
    console.log(`job ${jobName} succeeded`);

};

const resourceUpdated = res => async ({ identifier, url, force }) => {
    const commits = await getCommits();
    if(!commits[res] || commits[res] != identifier || force) {
        await setCommit(res, identifier);
        await setUrl(res, url);
        if(!Object.values(commits).includes(null)) {
            getBuild().then(async build => {
                await spawnJob(build);
            });
        }
    }
};

// Subscriptions
resources.map(res => nats.subscribe(res, resourceUpdated(res)));

console.log(tasks);

