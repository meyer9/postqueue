const { Queue } = require('../dist')
const Knexfile = require('./knexfile')
const knex = require('knex')

const TestQueue = new Queue('test-queue', knex(Knexfile['development']))

// add 10000 jobs
for (let i = 0; i < 10; i++) {
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
for (let i = 0; i < 1; i++) {
  TestQueue.process(async (j) => {
    if (!first) first = new Date()
    last = new Date()
    num++

    if (num === 5) {
      throw new Error('ugh')
    }

    // log info
    console.log(`${last - first} ms - processed ${num}`)
  })
}

