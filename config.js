module.exports = {
    name: process.env.NAME || 'pipeline.mypipe.job.myjob',
    resources: (process.env.RESOURCES || 'res-a:res-b:res-c').split(':'),
    tasks: {
        path: process.env.TASK_PATH || '/etc/config/tasks.json',
    },
    queue: {
        url: process.env.QUEUE_URL || 'localhost'
    },
    redis: {
        host: process.env.REDIS_HOST || 'localhost',
        port: process.env.REDIS_PORT || 6379
    },
    minio: {
        bucket: 'job-output'
    }
};

