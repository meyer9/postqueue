const { Queue } = require('../dist')
const Knexfile = require('./knexfile')
const knex = require('knex')

const TestQueue = new Queue('test-queue', knex(Knexfile['development']))

TestQueue.process(async (j) => {
  console.log(`processing: ${JSON.stringify(j.data)}`)
  return {
    gotback: 'data'
  }
});

(async () => {
  // normal job
  const job = await TestQueue.add({
    test: 'hello!',
  }, {
    deleteOnAcknowledged: false
  })

  console.log(`processed: ${JSON.stringify(await job.done())}`)

  // recurring job
  await TestQueue.add({
    test: 'recur hello!',
  }, {
    everySecs: 5 // runs every 5 seconds
  })
})()
