const cfg = require('./config');
const fs = require('fs');
const redis = require('async-redis').createClient({ host: cfg.redis.host, port: cfg.redis.port });
const nats = require('nats').connect(cfg.queue.url, { json: true });

const fromEntries = l => l.reduce((a, [k,v]) => ({...a, [k]: v}), {});
const zip = (a, b) => a.map((k, i) => [k, b[i]]);

// Load tasks from file
//const tasks = JSON.parse(fs.readFileSync(cfg.tasks.path));

// This function atomically updates the resources cache given the update object,
// it returns the new updated cache, and whether or not any changes were actually
// made.
const updateCache = async (update = {}) => new Promise((resolve, reject) => {
    var multi = redis.multi();

    multi = cfg.resources.reduce((multi, res) => multi.hgetall(`${cfg.name}.${res}`), multi);
    const l0 = multi.queue.length;

    multi = cfg.resources.reduce((multi, res) => {
        return Object.keys(update[res] || {})
            .reduce((multi, field) => multi.hset(`${cfg.name}.${res}`, field, update[res][field]), multi);
    }, multi);
    const l1 = multi.queue.length - l0;

    multi = cfg.resources.reduce((multi, res) => multi.hgetall(`${cfg.name}.${res}`), multi);
    const l2 = multi.queue.length - l1 - l0;

    multi.exec((error, result) => {
        if(error) {
            reject(error);
        }
        const pre = fromEntries(zip(cfg.resources, result.slice(0,-l1-l2).map(r => r || {})));
        const cache = fromEntries(zip(cfg.resources, result.slice(l0 + l1).map(r => r || {})));
        const unchanged = JSON.stringify(pre) === JSON.stringify(cache);
        resolve({ changed: !unchanged, cache });
    });
});

// Gets a new build number and updates the cache with the details of every resource entering
// the build.
const updateBuildCache = async (cache) => new Promise((resolve, reject) => {
    redis.incr(`${cfg.name}.build`)
        .then((build) => {
            var multi = cfg.resources.reduce((multi, res) => {
                return Object.keys(cache[res])
                    .reduce((muli, field) => multi.hset(`${cfg.name}.build.${build}.${res}`, field, cache[res][field]), multi);
            }, redis.multi());
            const skip = multi.queue.length;

            multi = cfg.resources.reduce((multi, res) => multi.hgetall(`${cfg.name}.build.${build}.${res}`), multi);

            multi.exec((error, result) => {
                if(error) {
                    return reject(error);
                }

                resolve({
                    build,
                    resources: fromEntries(zip(cfg.resources, result.slice(skip).map(r => r || {})))
                });
            });
        })
        .catch((error) => {
            reject(error);
        });
});


const resourceUpdated = res => async ({ identifier, bucket, object }) => {
    const check = (cache) => cfg.resources.reduce((acc, res) => {
        return acc && cache[res] && cache[res].identifier && cache[res].bucket && cache[res].object;
    }, true);

    if(!identifier || !bucket || !object) {
        console.error(`invalid message`);
        console.error(JSON.stringify({ identifier, bucket, object }));
        return;
    }

    const { changed, cache } = await updateCache({ [res]: { identifier, bucket, object } });

    if(changed && check(cache)) {
        await updateBuildCache(cache).then(startBuild);
    }
};

const startBuild = async (build) => {
    for(i = 0 ; i < tasks.length ; ++i) {
        const task = tasks[i];
        console.log(`spawning task: ${task}`);
        const promise = new Promise((resolve, reject) => {
            const subject = `${cfg.name}.task.${task.name}.start`;
            const options = { max: 1 };
            const data = {
                ...task,
                ...build,
                output: {
                    bucket: cfg.minio.bucket,
                    object: `${cfg.name}.task.${task.name}.build.${build.build}`
                }
            };
            nats.request(subject, data, options, ({status}) => {
                if(status == 'failed') {
                    return reject(status);
                }
                return resolve(status);
            });
        });
        await promise.catch((status) => {
            nats.publish(cfg.name, { build: build.build, status: 'failed' });
            throw status;
        });
    }
    nats.publish(cfg.name, { build: build.build, status: 'succeeded' });
};

cfg.resources.map(res => nats.subscribe(res, resourceUpdated(res)));
