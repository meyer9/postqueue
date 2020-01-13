const { Queue } = require('../dist')
const Knexfile = require('./knexfile')
const knex = require('knex')

const TestQueue = new Queue('test-queue', knex(Knexfile['development']))

// add 10000 jobs
for (let i = 0; i < 10000; i++) {
  TestQueue.add({
    test: 'hello!',
    n: i
  }).then(j => console.log('added', j.id))
}

// keep track of first processing time and last processing time
let first = undefined
let last = undefined

// keep track of number processed
let num = 0

// start 100 processors
for (let i = 0; i < 100; i++) {
  TestQueue.process(async (j) => {
    if (!first) first = new Date()
    last = new Date()
    num++

    // log info
    console.log(`${last - first} ms - processed ${num}`)
  })
}

