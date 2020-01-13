import * as Knex from 'knex'
import { TableOptions } from './migrations'
import Debug from 'debug'

const debug = Debug('loqueue:queue')

interface JobOptions {
  deleteOnAcknowledged?: boolean;
  everySecs?: number;
}

/**
 * Represents a single or recurring job by ID.
 */
export class Job {
  id: number
  knex: Knex;
  tableOptions: TableOptions;
  pollMs: number;

  constructor (id: number, knex: Knex, tableOptions: TableOptions, pollMs: number) {
    this.id = id
    this.knex = knex
    this.tableOptions = tableOptions
    this.pollMs = pollMs
  }

  /**
   * Waits for a job to complete (only applicable to non-recurring jobs)
   */
  async done (): Promise<any> {
    return new Promise((resolve, reject) => {
      const int = setInterval(async () => {
        const res = await this.knex.delete()
          .from(this.tableOptions.resultTableName)
          .where({
            job_id: this.id
          })
          .returning('*')

        if (res.length > 0) {
          clearInterval(int)

          await this.knex.delete()
            .from(this.tableOptions.tableName)
            .where({ id: this.id })

          resolve(res[0].result)
        }
      }, this.pollMs)
    })
  }

  /**
   * Removes job from queue
   */
  async remove (): Promise<void> {
    await this.knex.delete()
      .where({ id: this.id })
      .from(this.tableOptions.tableName)
  }
}

export interface QueueOptions extends TableOptions {
  pollInterval: number;
}

/**
 * This represents a single queue.
 */
export class Queue {
  name: string
  knex: Knex
  tableOptions: TableOptions;
  pollInterval: number;

  constructor (name: string, knex: Knex, options?: Partial<QueueOptions>) {
    this.name = name
    this.knex = knex
    const opts = Object.assign({
      tableName: 'loqueue',
      resultTableName: 'loqueue_results',
      pollInterval: 1000
    }, options)

    this.tableOptions = {
      tableName: opts.tableName,
      resultTableName: opts.resultTableName
    }

    this.pollInterval = opts.pollInterval
  }

  /**
   * Adds a single or recurring job to the queue
   * @param data Job data
   * @param options Job options
   */
  async add (data: any, options?: JobOptions): Promise<Job> {
    const opts = Object.assign({
      everySecs: undefined,
      deleteOnAcknowledged: true
    }, options)

    if (opts.everySecs && !opts.deleteOnAcknowledged) {
      console.warn('deleteOnAcknowledged can only be false for single jobs. We always delete results for recurring jobs to prevent database clutter. Instead, you should provide results manually from inside the processing function.')
      opts.deleteOnAcknowledged = true
    }

    const id = await this.knex.insert({
        input_data: data,
        queue_name: this.name,
        every_secs: opts.everySecs,
        delete_on_acknowledged: opts.deleteOnAcknowledged,
        last_run: this.knex.raw('NOW()')
      })
      .returning('id')
      .into(this.tableOptions.tableName)

    return new Job(id[0], this.knex, this.tableOptions, this.pollInterval)
  }

  /**
   * Gets a Job object from the queue.
   * @param id Job ID number
   */
  getJob (id: number): Job {
    return new Job(id, this.knex, this.tableOptions, this.pollInterval)
  }

  /**
   * Process runs the queue and processes jobs.
   * @param cb Callback to handle a job. Return value should be job result.
   */
  process (cb: (j: Job) => Promise<any>): void {
    const knex = this.knex

    const toRun = () => {
      knex.transaction(async trx => {
        const q = trx
          .select(knex.raw('*'))
          .forUpdate()
          .skipLocked()
          .from(this.tableOptions.tableName)
          .limit(1)
          .where('queue_name', this.name)
          .andWhere(function () {
            this.where('last_run', '<', knex.raw('NOW() - every_secs * interval \'1 seconds\''))
            this.orWhereNull('every_secs')
          })
        const jobs = await q

        debug(`got ${jobs.length} jobs`)

        if (jobs.length > 0) {
          const job = jobs[0]

          debug(`acquired lock for job ${job.id}`)

          const out = await cb(job.input_data)

          debug(`finished job ${job.id}, releasing lock`)

          if (job.every_secs) {
            await trx.update({
              last_run: knex.raw(`
              CASE WHEN (last_run + every_secs * interval \'1 seconds\' > NOW() - every_secs * interval \'1 seconds\') THEN
                last_run + every_secs * interval \'1 seconds\'
                ELSE NOW()
              END`)
            }).where({
              id: job.id
            }).into(this.tableOptions.tableName)
          } else {
            if (!job.delete_on_acknowledged) { // delete_on_acknowledged == false implies they are expecting ack
              await trx.insert({
                job_id: job.id,
                result: out,
                time_run: new Date()
              }).into(this.tableOptions.resultTableName)
            }
            await trx.delete().where({
              id: job.id
            }).from(this.tableOptions.tableName)
          }

          setImmediate(toRun)
        } else {
          setTimeout(toRun, this.pollInterval)
        }
      })
    }
    setImmediate(toRun)
  }
}
