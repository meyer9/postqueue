# loqueue

loqueue is a simple postgres-backed queue. Currently, it can process about 1360 jobs per second with a tiny bit of overhead.

## Installation

```bash
yarn add loqueue
```

## Usage

```javascript
const { Queue } = require('loqueue')
const Knexfile = require('./knexfile')
const knex = require('knex')

const TestQueue = new Queue('test-queue', knex(Knexfile['development']))

TestQueue.process(async (j) => {
  console.log(`processing: ${JSON.stringify(j)}`)
  return {
    gotback: 'data'
  }
});

(async () => {
  // normal job, will not delete results until done()
  const job = await TestQueue.add({
    test: 'hello!',
  }, {
    deleteOnAcknowledged: false
  })

  // this will print out the output from the job
  console.log(`processed: ${JSON.stringify(await job.done())}`)

  // recurring job, will not save results
  await TestQueue.add({
    test: 'recur hello!',
  }, {
    everySecs: 5 // runs every 5 seconds
  })
})()
```

## Migrations

Migrations can be done using `setupTables` and `dropTables`:

```javascript
const { setupTables, dropTables } = require('../../dist')

exports.up = function(knex) {
  return setupTables(knex)
};

exports.down = function(knex) {
  return dropTables(knex)
};
```
