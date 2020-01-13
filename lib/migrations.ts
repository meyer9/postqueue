import * as Knex from 'knex'

export interface TableOptions {
  tableName: string;
  resultTableName: string;
}

/**
 * Sets up tables in a PostgreSQL database.
 * @param knex Knex instance to use to setup tables.
 * @param options Specifies table names and other options
 */
export async function setupTables (knex: Knex, options?: Partial<TableOptions>): Promise<any> {
  const newOptions = Object.assign({
    tableName: 'loqueue',
    resultTableName: 'loqueue_results'
  }, options)
  await knex.schema.createTable(newOptions.tableName, tbl => {
    tbl.increments('id')
    tbl.json('input_data')
    tbl.string('queue_name').notNullable()
    tbl.integer('every_secs')
    tbl.dateTime('last_run')
    tbl.boolean('delete_on_acknowledged').notNullable()
  })
  await knex.schema.createTable(newOptions.resultTableName, tbl => {
    tbl.increments('id')
    tbl.integer('job_id').notNullable().unsigned()
    tbl.json('result')
    tbl.dateTime('time_run')
  })
}

/**
 * Drops tables from PostgreSQL database.
 * @param knex Knex instance to use to drop tables
 * @param options Specifies table names
 */
export async function dropTables (knex: Knex, options?: Partial<TableOptions>): Promise<any> {
  const newOptions = Object.assign({
    tableName: 'loqueue',
    resultTableName: 'loqueue_results'
  }, options)
  await knex.schema.dropTable(newOptions.resultTableName)
  await knex.schema.dropTable(newOptions.tableName)
}
