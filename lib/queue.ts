import * as Knex from 'knex'
import { TableOptions } from './migrations'
import Debug from 'debug'

const debug = Debug('postqueue:queue')

interface JobOptions {
  deleteOnAcknowledged?: boolean;
  everySecs?: number;
}

/**
 * Represents a single or recurring job by ID.
 * @property {number} id Job ID number
 * @property {object} data Job data
 * @param {number} id ID number of job
 * @param {Knex} knex Knex instance to use
 * @param {TableOptions} tableOptions Table options including names
 * @param {number} pollMs Poll interval
 * @param {Knex.Transaction} tx Transaction if applicable (inside processing function)
 */
export class Job {
  id: number

  private knex: Knex;
  private tx?: Knex.Transaction;
  private tableOptions: TableOptions;
  private pollMs: number;

  data: any;

  /**
   * Please use Queue.getJob(jobID) to get a job. This constructor is
   * for internal use only.
   */
  constructor (id: number, knex: Knex, tableOptions: TableOptions, pollMs: number, tx?: Knex.Transaction) {
    this.id = id
    this.knex = knex
    this.tableOptions = tableOptions
    this.pollMs = pollMs
    this.tx = tx
  }

  /**
   * Waits for a job to complete (only applicable to non-recurring jobs)
   */
  async done (): Promise<any> {
    if (this.tx) {
      throw new Error('Can\'t wait for job to finish inside processing function')
    }
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
    if (this.tx) {
      await this.tx.delete()
        .where({ id: this.id })
        .from(this.tableOptions.tableName)
    } else {
      await this.knex.delete()
        .where({ id: this.id })
        .from(this.tableOptions.tableName)
    }
  }
}

export interface QueueOptions extends TableOptions {
  pollInterval: number;
}

/**
 * This represents a single queue.
 * @param {object} data Job data
 * @param {TableOptions} options Job options
 */
export class Queue {
  private name: string
  private knex: Knex
  private tableOptions: TableOptions;
  private pollInterval: number;
  private shouldProcess: boolean;

  constructor (name: string, knex: Knex, options?: Partial<QueueOptions>) {
    this.name = name
    this.knex = knex
    this.shouldProcess = true
    const opts = Object.assign({
      tableName: 'postqueue',
      resultTableName: 'postqueue_results',
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
   * @param {object} data Job data to be passed to the processing function
   * @param {JobOptions} options Job options for configuring repeat and output handling
   * @returns {Job} Created job
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
   * @param {number} id Job ID number
   * @returns {Job} Job retrieved
   */
  getJob (id: number): Job {
    return new Job(id, this.knex, this.tableOptions, this.pollInterval)
  }

  /**
   * Process runs the queue and processes jobs.
   * @param {Function} cb Callback to handle a job. Return value should be job result.
   */
  process (cb: (j: Job) => Promise<any>): void {
    const knex = this.knex
    this.shouldProcess = true

    const toRun = () => {
      knex.transaction(async trx => {
        try {
          const jobs = await trx
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

          debug(`got ${jobs.length} jobs`)

          if (jobs.length > 0) {
            const job = jobs[0]

            debug(`acquired lock for job ${job.id}`)

            const jobPassed = new Job(job.id, knex, this.tableOptions, this.pollInterval, trx)
            jobPassed.data = job.input_data

            const out = await cb(jobPassed)

            debug(`finished job ${job.id}, releasing lock`)

            if (job.every_secs) {
              debug('updating last run')
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
                debug('adding result')
                await trx.insert({
                  job_id: job.id,
                  result: out,
                  time_run: new Date()
                }).into(this.tableOptions.resultTableName)
              }
              debug('deleting job')
              await trx.delete().where({
                id: job.id
              }).from(this.tableOptions.tableName)
            }

            await trx.commit()

            if (this.shouldProcess) {
              setImmediate(toRun)
            }
          } else {
            if (this.shouldProcess) {
              setTimeout(toRun, this.pollInterval)
            }
          }
        } catch (err) {
          debug(`caught error: ${err.message}`)

          if (this.shouldProcess) {
            setTimeout(toRun, this.pollInterval)
          }

          throw err
        }
      })
    }
    setImmediate(toRun)
  }

  shutdown () {
    this.shouldProcess = false
  }
}
