import { Queue, Job } from './queue'
import { expect } from 'chai'
import knex from 'knex'
import 'mocha'
import { setupTables } from './migrations'

const db = knex({
  client: 'postgresql',
  connection: {
    database: 'postqueue_test' || process.env.POSTGRES_DB,
    user:     'postgres' || process.env.POSTRGES_USER,
    password: '' || process.env.POSTGRES_PASS
  }
})

describe('queue', () => {
  before(async () => {
    const tablesExist = await db.schema.hasTable('postqueue')
    if (!tablesExist) {
      await setupTables(db)
    }
  })

  it('should successfully add and delete', async () => {
    const q = new Queue('test-queue', db)

    const job = await q.add({
      test: 'hello-world'
    })

    await job.remove()
  })

  it('should process jobs', (done) => {
    (async () => {
      const q = new Queue('test-queue', db)

      await q.add({
        test: 'hello-world'
      })

      q.process(async (j) => {
        expect(j.data).to.not.be.equal(null)
        expect(j.data.test).to.be.equal('hello-world')
        q.shutdown()
        await j.remove()
        done()
        return null
      })

    })()
  })

  it('should process recurring jobs multiple times', (done) => {
    (async () => {
      const q = new Queue('test-queue', db)

      let triggered = 0

      const job = await q.add({
        test: 'hello-world'
      }, {
        everySecs: 1
      })

      q.process(async (j: Job) => {
        expect(j.data).to.not.be.equal(null)
        expect(j.data.test).to.be.equal('hello-world')
        triggered++

        if (triggered === 2) {
          q.shutdown()
          await j.remove()
          done()
        }
        return null
      })
    })()
  }).timeout(5000)

  after(() => {
    setImmediate(() => {
      db.destroy()
    })
  })
})

