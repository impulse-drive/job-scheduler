// Configuration
const NAME = process.env.NAME || 'job1';
const QUEUE_URL = process.env.QUEUE_URL || 'localhost:4444';
const STORAGE_URL = process.env.STORAGE_URL || 'http://localhost:3000';
const RESOURCES = process.env.RESOURCES || 'home-manager';
const PIPELINE = process.env.PIPELINE || 'pipeline';
const DATA_DIR = process.env.DATA_DIR || `/tmp/${PIPELINE}/job/${NAME}`
const JOB = process.env.JOB || '{"name":"job1"}'

// Imports
const request = require('request'),
      path = require('path'),
      mkdirp = require('mkdirp'),
      fs = require('fs'),
      compressing = require('compressing'),
      rimraf = require('rimraf');

// Connections
const k8s = new (require('kubernetes-client').Client)({ version: '1.13' });
const nats = require('nats').connect(QUEUE_URL, { json: true });

// Common functions
Object.fromEntries = l => l.reduce((a, [k,v]) => ({...a, [k]: v}), {});
Object.allValuesNotNull = l => Object.values(l).reduce((a, v) => a && (v!=null), true);

const resourcePath = identifier =>
    path.join(DATA_DIR, `resource/${identifier}.tar.gz`);

const resourcePathDecompressed = res =>
    path.join(DATA_DIR, `resource/${res}`)

// Initial state
const job = JSON.parse(JOB);
var resources = Object.fromEntries(RESOURCES.split(':').map(x => [x, null]));

// Resource handle
const spawnJob = async () => {

    const extract = async () => {
        Object.entries(resources).map(async ([res, { identifier }]) => {
            const filename = resourcePath(identifier);
            const dir = resourcePathDecompressed(res);
            if(fs.existsSync(dir)) {
                rimraf.sync(dir);
            }
            await mkdirp(dir);
            await compressing.tgz.uncompress(filename, dir);
            console.log(`decompressed ${filename} to ${dir}`);
        });
    };

    await extract();
    console.log(JSON.stringify(job));
};

const resourceUpdated = res => async ({ identifier, url }) => {

    const download = () => new Promise((resolve, reject) => {
        console.log({identifier, url});
        const filename = resourcePath(identifier);
        const dl = () => request.get(url).pipe(fs.createWriteStream(filename))
            .on('close',  () => resolve(filename))
            .on('error', err => reject(err));
        mkdirp(path.dirname(filename)).then(dl).catch(reject);
    });

    if(!resources[res] || resources[res].identifier != identifier) {
        const filename = await download();
        resources[res] = { identifier, filename };
        if(Object.allValuesNotNull(resources)) {
            await spawnJob();
        }
    }
};

// Subscriptions
Object.keys(resources).map(res => {
    nats.subscribe(`${PIPELINE}/resource/${res}`, resourceUpdated(res));
});

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
